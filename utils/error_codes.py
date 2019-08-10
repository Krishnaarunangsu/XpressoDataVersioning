# Server Error
server_error = 100
# Login Codes
already_logged_in = 101
no_user = 102
wrong_pwd = 103
empty_uid = 104
empty_pwd = 105
auth_failed = 106

# Token & Permission Codes
wrong_token = 111
expired_token = 112
permission_denied = 113

# Cluster Codes
cluster_not_found = 121
cluster_already_exists = 122
incomplete_cluster_info = 123
cluster_name_blank = 124

# User management codes
username_already_exists = 131
user_exists = 132
user_not_found = 133
incomplete_user_information = 134
incorrect_primaryRole = 135
user_already_deactivated = 136
cannot_modify_password = 137
call_deactivate_user = 138
password_not_valid = 139

# Node management codes
node_already_exists = 141
incomplete_node_information = 142
node_unavailable = 143
node_not_found = 144
kubernetes_error = 145
invalid_node_data = 146
incomplete_provision_information = 147
invalid_provision_information = 148
invalid_node_type = 149
node_assign_failed = 150
invalid_master_node = 211
master_not_provisioned = 212
node_already_provisioned = 213
node_already_deactivated = 214
call_deactivate_node = 215
node_already_assigned = 216
node_not_provisioned = 217
create_cluster_first = 218
node_provision_failed = 219

# Project Management Codes
incomplete_project_info = 151
project_not_found = 152
project_build_failed = 153
deployment_creation_failed = 154
project_deployment_failed = 155
project_undeployment_failed = 156
invalid_build_version = 157
components_specified_incorrectly = 158
service_creation_failed = 159
namespace_creation_failed = 160
branch_not_specified = 161
currently_not_deployed = 162
port_patching_attempted = 163
job_creation_failed = 164
cronjob_creation_failed = 165
invalid_cron_schedule = 166
invalid_job_type = 167
invalid_job_commands = 168

# Project management codes
incomplete_project_information = 171
invalid_project_format = 172
invalid_project_field_format = 173
project_already_exists = 174
projectname_already_exists = 175
project_not_created = 176
project_currently_deployed = 177
project_deactivation_failed = 178
component_already_exists = 179
activate_project_first = 180
call_deactivate_method = 181
invalid_owner_information = 182
invalid_developer_information = 183
developer_not_found = 184
internal_config_error = 185
unknown_component_key = 186
invalid_component_format = 187
incorrect_component_fields = 188
project_setup_failed = 189
repo_clone_failed = 190
incorrect_pipelines_information = 191

# Bitbucket transaction management codes
project_creation_failed = 192
repo_creation_failed = 193
skeleton_repo_creation_failed = 194
project_push_failed = 195

# General Xpresso Errors
blank_field_error = 201
missing_field_error = 202
invalid_value_error = 203
file_not_found = 204
json_load_error = 205

# Database Operation Codes
unsuccessful_connection = 221
unsuccessful_operation = 222

# Kubeflow Error Codes
declarative_json_incorrect = 231
reference_not_found = 232
pipeline_not_found = 233
ambassador_port_fetching_failed = 234
pipeline_upload_failed = 235

# API Gateway
gateway_connection_error = 241
gateway_duplicate_error = 242

# deployment environments
invalid_environment_error = 251
no_clusters_present_error = 252
incorrect_deployment_error = 253

# Serialization
serialization_failed = 261
deserialization_failed = 262

# Pachyderm Repo Management error codes
pachyderm_repo_not_provided = 301
dataset_info_error = 302
dataset_path_invalid = 303
pachyderm_branch_info_error = 304
pachyderm_field_name_error = 305
pachyderm_operation_error = 306
local_path_exception = 307
