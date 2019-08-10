""" User Metrics class"""
from xpresso.ai.admin.controller.metrics.abstract_metrics import AbstractMetrics

__all__ = ["ProjectMetrics"]
__author__ = ["Naveen Sinha"]


class ProjectMetrics(AbstractMetrics):
    """
    Fetches all the details for Projects
    """

    def __init__(self, config, persistence_manager):
        super().__init__(config=config,
                         persistence_manager=persistence_manager)

    def metric_project(self):
        """ get count of all users, active users, inactive users"""
        total_projects = self.persistence_manager.find(collection="projects",
                                                       doc_filter={})
        deployed_project = 0
        total_builds = 0
        for project in total_projects:
            if "currentlyDeployed" in project and project["currentlyDeployed"]:
                deployed_project += 1
            if "components" in project and project["components"]:
                for component in project["components"]:
                    total_builds += len(component["versions"])

        final_metric = [("total_projects", len(total_projects)),
                        ("deployed_projects", deployed_project),
                        ("total_builds", total_builds)]
        return self.format_response(final_metric)

    def metric_event_list(self):
        self.persistence_manager.connect()
        total_deploy_events = self.persistence_manager.find(
            collection="events",
            doc_filter={
                "request_type": "/projects/deploy",
                "response.outcome": "success"
            }
        )
        print((total_deploy_events))
        unique_deploy_projects = self.extract_project_name(total_deploy_events)
        total_build_events = self.persistence_manager.find(
            collection="events",
            doc_filter={
                "request_type": "/projects/build",
                "response.outcome": "success"
            }
        )
        print(total_build_events)
        unique_build_projects = self.extract_project_name(total_build_events)

        final_metric = [
            ("last_ten_deployed_projects", self.find_last_n_unique_item(
                unique_deploy_projects,
                10)),
            ("last_ten_built_projects", self.find_last_n_unique_item(
                unique_build_projects,
                10))
             ]
        return self.format_response(final_metric)

    @staticmethod
    def extract_project_name(total_build_events):
        return [item["request_json"]["name"] for item in total_build_events
                if "request_json" in item and "name" in item["request_json"]]
