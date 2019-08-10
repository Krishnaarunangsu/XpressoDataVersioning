""" User Metrics class"""
from xpresso.ai.admin.controller.metrics.abstract_metrics import AbstractMetrics

__all__ = ["UserMetrics"]
__author__ = ["Naveen Sinha"]


class UserMetrics(AbstractMetrics):
    """
    Fetches all the details for Users
    """

    def __init__(self, config, persistence_manager):
        super().__init__(config=config,
                         persistence_manager=persistence_manager)

    def metric_users(self):
        """ get count of all users, active users, inactive users"""
        total_user = self.persistence_manager.find(collection="users",
                                                   doc_filter={})
        inactive_count = 0
        active_count = 0
        for user in total_user:
            if "loginStatus" not in user or not user["loginStatus"]:
                inactive_count += 1
                continue
            active_count += 1

        final_metric = [("total_users", len(total_user)),
                        ("active_users", active_count),
                        ("inactive_users", inactive_count)]
        return self.format_response(final_metric)

    def metric_event_list(self):
        self.persistence_manager.connect()
        total_events = self.persistence_manager.find(
            collection="events",
            doc_filter={
                "request_type": "/users"
            }
        )
        total_events.reverse()
        unique_uid = [item["user"]["uid"] for item in total_events
                      if "user" in item and "uid" in item["user"]]
        final_metric = [
            ("last_ten_active_users",
             self.find_last_n_unique_item(unique_uid, 10))]
        return self.format_response(final_metric)
