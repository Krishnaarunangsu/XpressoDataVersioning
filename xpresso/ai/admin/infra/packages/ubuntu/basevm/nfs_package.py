"""Abstract base class for packages object"""

__all__ = ['NFSPackage']
__author__ = 'Naveen Sinha'

import re

from xpresso.ai.admin.infra.packages.abstract_package import AbstractPackage
from xpresso.ai.admin.infra.packages.local_shell_executor import \
    LocalShellExecutor
from xpresso.ai.core.utils.xpr_config_parser import XprConfigParser
from xpresso.ai.core.logging.xpr_log import XprLogger
from xpresso.ai.core.utils import linux_utils
from xpresso.ai.admin.controller.exceptions.xpr_exceptions import\
    CommandExecutionFailedException


class NFSPackage(AbstractPackage):
    """
    Mount NFS network drive into the current linux VM
    """

    NFS_SECTION = "nfs"
    SUBNET_MAP_KEY = "subnet_to_nfs_map"
    NFS_LOCATION_KEY = "nfs_location"
    MOUNT_LOCATION_KEY = "mount_location"

    FSTAB_FILE = "/etc/fstab"

    def __init__(self, config_path=XprConfigParser.DEFAULT_CONFIG_PATH,
                 executor=None):
        if not executor:
            executor = LocalShellExecutor()
        super().__init__(executor)
        self.config = XprConfigParser(config_path)["packages_setup"]

    def execute(self, **kwargs):
        """
        Mounts the NFS on the VM
        """
        logger = XprLogger()
        if not linux_utils.check_root():
            logger.fatal("Please run this as root")
        logger.info("Mounting NFS File")
        subnet_to_nfs_map = self.config[self.NFS_SECTION][self.SUBNET_MAP_KEY]
        ip_address = linux_utils.get_ip_address()
        matched_nfs = None
        for nfs, subnet in subnet_to_nfs_map.items():
            logger.info("Matching {} {}".format(subnet, ip_address))
            check = re.match(subnet, ip_address)
            print(check)
            if check:
                matched_nfs = nfs
                break

        if not matched_nfs:
            logger.info("Could not determine nfs value")
            return False

        mount_location = self.config[self.NFS_SECTION][self.MOUNT_LOCATION_KEY]
        nfs_location = self.config[self.NFS_SECTION][self.NFS_LOCATION_KEY]
        mount_script = "mount {}:{} {}".format(matched_nfs, nfs_location,
                                               mount_location)
        logger.info("Mounting {}".format(mount_script))
        try:
            linux_utils.create_directory(mount_location,0o755)
            self.executor.execute(mount_script)
            logger.info("Mount Succesful")
            logger.info("Updating fstab file")
            with open(self.FSTAB_FILE, "a+") as f:
                fstab_statement = "{}:{}    {}   nfs " \
                                  "auto,nofail,noatime,nolock,intr,tcp," \
                                  "actimeo=1800 0 0" \
                    .format(matched_nfs, nfs_location, mount_location)
                logger.info(
                    "Updating fstab file with {}".format(fstab_statement))
                f.write(fstab_statement)
                logger.info("Update Successful")
        except CommandExecutionFailedException as e:
            logger.error("Script Failed to run = {}\n{}".format(mount_script,
                                                                str(e)))
            return False
        return True

    def status(self, **kwargs):
        pass

    def cleanup(self, **kwargs):
        pass

    def install(self, **kwargs):
        dependencies_script = "sudo apt-get install -y nfs-common"
        self.executor.execute(dependencies_script)
        self.execute()

    def uninstall(self, **kwargs):
        self.cleanup()

    def start(self, **kwargs):
        self.execute()

    def stop(self, **kwargs):
        self.cleanup()


if __name__ == "__main__":
    nfs_cmd = NFSPackage()
    nfs_cmd.execute()
