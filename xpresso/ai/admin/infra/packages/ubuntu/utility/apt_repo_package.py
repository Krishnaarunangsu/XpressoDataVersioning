"""Abstract base class for packages object"""

__all__ = ['AptRepositoryPackage']
__author__ = 'Naveen Sinha'

import glob
import os
import shutil

import docker
import tqdm

from xpresso.ai.admin.infra.packages.abstract_package import AbstractPackage
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.admin.infra.packages.local_shell_executor import \
    LocalShellExecutor
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import \
    CommandExecutionFailedException


class AptRepositoryPackage(AbstractPackage):
    """
    Sets up private aptitude repository on ubuntu VM

    """

    # Configuration Keys
    APT_SECTION = "apt-get-repo"
    NFS_PACKAGE_KEY = "nfs_package_folder"
    APT_PUBLIC_KEY = "public_key_file"
    APT_PRIVATE_KEY = "private_key_file"
    APT_HOSTED_PACKGE_KEY = "hosted_package"
    PACKAGE_LIST_KEY = "package-list"
    DOCKER_NAME = "docker-name"
    META_PACKAGE_KEY = "meta_packages_folder"
    DOCKER_FILE_PATH_KEY = "dockerfile-path"

    DOCKER_IMAGE_VERSION = 0.1

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH,
                 executor=None):
        if not executor:
            executor = LocalShellExecutor()
        super().__init__(executor=executor)

        self.config = XprConfigParser(config_path)["packages_setup"]
        self.logger = XprLogger()

        self.apt_config = self.config[self.APT_SECTION]
        self.public_key = self.apt_config[self.APT_PUBLIC_KEY]
        self.private_key = self.apt_config[self.APT_PRIVATE_KEY]
        self.hosted_package_folder = self.apt_config[self.APT_HOSTED_PACKGE_KEY]
        self.sign_paraphrase = None
        self.sign_key_id = None
        self.home_folder = os.getcwd()

    def execute(self, parameters=None):
        if parameters:
            self.sign_paraphrase = parameters["paraphrase"]
            self.sign_key_id = parameters["key_id"]
        self.cleanup()
        self.pre_install()
        self.download_dependencies()
        self.setup_meta_packages()
        self.sign_packages()
        self.run_docker_container()

    def download_dependencies(self):
        """
        Generates the list of packages and all its dependencies. Download the
        packages into the directory

        install apt-rdepends to generate the list
        install apt-download to download the package
        Ignore error
        """

        with open(self.apt_config[self.PACKAGE_LIST_KEY]) as pkg_fp:
            pkg_list = pkg_fp.read().splitlines()

        if not pkg_list:
            return None
        os.chdir(self.home_folder)
        self.logger.info("Generating all dependencies")
        full_package_list = []
        for pkg in tqdm.tqdm(pkg_list):
            script = 'apt-rdepends {} |  grep -v "^ "'.format(pkg)
            self.logger.info(script)
            try:
                (_, output, _) = self.executor.execute_with_output(script)
                dependencies = output.splitlines()
                full_package_list += [x.decode() for x in dependencies]
            except CommandExecutionFailedException:
                self.logger.warning("Package fetch failed")
        full_package_set = set(full_package_list)
        # We have now full list. Download each of the dependencies.
        try:
            os.makedirs(self.hosted_package_folder, exist_ok=True)
            os.chdir(self.hosted_package_folder)
        except OSError:
            self.logger.info("Installation makes sense")

        self.logger.info("Download all dependencies => {}".format(os.getcwd()))
        self.logger.info(full_package_set)
        for pkg in tqdm.tqdm(list(full_package_set)):
            try:
                self.executor.execute(
                    "sudo -u xprops apt-get download {}".format(pkg))
            except CommandExecutionFailedException:
                self.logger.warning("Failed to download package {}".format(pkg))

    def setup_meta_packages(self):
        """
        Create meta package folder and build
        """
        os.chdir(self.home_folder)
        local_meta_folder = "{}/*.ns-control".format \
            (self.apt_config[self.META_PACKAGE_KEY])
        self.logger.info(local_meta_folder)
        for meta_pkg in glob.iglob(local_meta_folder, recursive=True):
            try:
                abs_meta_pkg = os.path.join(os.getcwd(), meta_pkg)
                meta_pkg_folder = os.path.join(self.hosted_package_folder,
                                               os.path.basename(meta_pkg).split(
                                                   '.')[0])
                self.logger.info(meta_pkg_folder)
                os.makedirs(meta_pkg_folder, exist_ok=True)
                os.chdir(meta_pkg_folder)
                shutil.copy(abs_meta_pkg, '.')
                build_meta_pkg_script = "sudo -u xprops equivs-build {}".format(
                    os.path.basename(meta_pkg))
                self.logger.info(build_meta_pkg_script)
                self.logger.info(os.getcwd())
                self.executor.execute(build_meta_pkg_script)
            except OSError as e:
                # Ignoring
                self.logger.error(e)
                self.logger.error("Failed to create meta {}".format(meta_pkg))

    def sign_packages(self):
        """
        Sign packages using private key

        """
        os.chdir(self.home_folder)
        try:
            self.executor.execute(
                'chmod 755 -R {}'.format(self.hosted_package_folder))
            self.logger.info("Importing Keys")
            self.executor.execute("gpg --import --batch {}".format(
                self.private_key))
            self.executor.execute('expect -c "spawn gpg --edit-key {} '
                                  'trust quit; send \"5\ry\r\"; expect eof"'
                                  .format(self.sign_key_id))
            os.chdir(self.hosted_package_folder)
            for deb_file in glob.iglob("{}/*.deb".format(
                self.hosted_package_folder), recursive=True):
                self.executor.execute(
                    'dpkg-sig -g "--pinentry-mode loopback --passphrase {}" '
                    '--sign builder {}'.format(self.sign_paraphrase, deb_file))
            self.executor.execute("apt-ftparchive packages . > Packages")
            self.executor.execute("gzip -c Packages > Packages.gz")
            self.executor.execute("apt-ftparchive release . > Release")
            self.executor.execute(
                'gpg --pinentry-mode loopback --passphrase {} '
                '--clearsign -o InRelease Release'
                    .format(self.sign_paraphrase))
            self.executor.execute(
                'gpg --pinentry-mode loopback --passphrase {} '
                '-abs -o Release.gpg Release'.format(
                    self.sign_paraphrase))

        except OSError:
            # Ignoring
            self.logger.error("Failed to sign {}")

    def run_docker_container(self):
        """
        Start the docker container
        """
        self.cleanup()
        os.chdir(self.home_folder)
        self.logger.info(os.getcwd())
        # Copy public key in local
        shutil.copy(self.public_key, './public_key')

        try:
            client = docker.from_env()
            docker_image_tag = ':'.join([self.apt_config[self.DOCKER_NAME],
                                         str(self.DOCKER_IMAGE_VERSION)])
            (_, build_log) = client.images.build(
                path=".",
                dockerfile=self.apt_config[self.DOCKER_FILE_PATH_KEY],
                tag=docker_image_tag,
                nocache=False
            )
            for line in build_log:
                self.logger.info(line)

            client.containers.run(
                image=docker_image_tag,
                name=self.apt_config[self.DOCKER_NAME],
                ports={"80/tcp": 8500},
                restart_policy={"Name": "on-failure",
                                "MaximumRetryCount": 5},
                volumes={
                    self.hosted_package_folder: {
                        'bind': '/usr/local/apache2/htdocs/deb', 'mode': 'rw'
                    }
                },
                detach=True)

        except (docker.errors.APIError, docker.errors.NotFound) as e:
            self.logger.error(e)
            self.logger.error("Could not build container".format(
                self.apt_config[self.DOCKER_NAME]))

    def pre_install(self):
        """
        Install required apt-get packages
        """
        try:
            self.executor.execute("apt-get update -y && "
                                  "apt-get -y install apt-rdepends "
                                  "dpkg-dev dpkg-sig expect apt-utils")
        except OSError:
            self.logger.error("Can not install the requirements")

    def cleanup(self, delete_packages=False):
        """
        Removes package and shutdown docker container
        """
        os.chdir(self.home_folder)
        if delete_packages:
            shutil.rmtree(self.hosted_package_folder)

        try:
            client = docker.from_env()
            apt_get_container = client.containers.get(
                self.apt_config[self.DOCKER_NAME])

            apt_get_container.stop()
            apt_get_container.remove()
        except (docker.errors.APIError, docker.errors.NotFound):
            self.logger.error("{} container failed to remove".format(
                self.apt_config[self.DOCKER_NAME]))

    def status(self):
        try:
            client = docker.from_env()
            apt_get_container = client.containers.get(
                self.apt_config[self.DOCKER_NAME])

            if apt_get_container.status == "running":
                return True
        except (docker.errors.APIError, docker.errors.NotFound):
            self.logger.error("{} container not found".format(
                self.apt_config[self.DOCKER_NAME]))
            return False
        return False

    def install(self, parameters=None):
        self.execute(parameters=parameters)

    def uninstall(self, **kwargs):
        self.cleanup(delete_packages=True)

    def start(self, **kwargs):
        self.execute()

    def stop(self, **kwargs):
        self.cleanup(delete_packages=False)


if __name__ == "__main__":
    cmd = AptRepositoryPackage()
    cmd.execute(parameters={"paraphrase": "abz00ba1nc ",
                            "key_id": "6DC5FF871CBBA7F5DE86D4F5D9EE73E4751A5FEC"})
