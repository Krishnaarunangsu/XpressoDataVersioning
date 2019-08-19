from xpresso.ai.core.data.structured_dataset import StructuredDataset
from xpresso.ai.core.data.pachyderm_repo_management.pachyderm_repo_manager import PachydermRepoManager

new_dataset = StructuredDataset()
data_path = "samples/age_count.csv"
new_dataset.import_dataset(data_path)

repo_manager = PachydermRepoManager()
push_to_repo_name = "abzooba_repo"
push_to_repo_branch = "abzooba_branch"
push_description = "sample push"
commit_id = repo_manager.push_dataset(push_to_repo_name, push_to_repo_branch, new_dataset, push_description)

