""" Report generator for promotheus """
from xpresso.ai.core.logging.xpr_log import XprLogger

__all__ = ["PromotheusMetricReportGenerator"]
__author__ = "Naveen Sinha"


class PromotheusMetricReportGenerator:
    """ Takes list of metrics and generate a promotheus compatible report """

    def __init__(self):
        self.logger = XprLogger()

    def generate_report(self, metrics):
        self.logger.info("converting metrics to promotheus report")
        report_list = []
        for metric in metrics:
            try:
                label = f"xpresso_ctrl_{metric['label']}"
                data = metric['data']
                if type(data) == list:
                    for data_point in data:
                        report_list.append(f'{label}{{value="{data_point}"}} 1')
                elif type(data) == int:
                    report_list.append(f'{label} {data}')
                elif type(data) == str:
                    report_list.append(f'{label} {data}')
            except KeyError:
                self.logger.debug("Ignoring the metric")
        self.logger.info("Report generation complete")
        return '\n'.join(report_list)
