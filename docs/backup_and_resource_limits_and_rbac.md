-------------------------------------------------------------------------------------------------------------------------
ORG, PROJECT, BRANCH
-------------------------------------------------------------------------------------------------------------------------

1. WE NEED NOW MAX_BACKUPS AND OPT. ENVS (COMMA-SEPARATED LIST) WHEN CREATING AN ORG OR CHANGING AN ORG
2. WE NEED ALSO MAX_BACKUPS WHEN CREATING OR CHANGING A PROJECT
3. WE NEED AN ENV_TYPE WHEN CREATING A BRANCH

curl -X POST "http://localhost:8000/vela/organizations/?response=full" \
-H "Authorization: Bearer $TOKEN" \
-H "Content-Type: application/json" \
-d '{
    "name": "VELA",
    "display_name": "VELA",
    "require_mfa": false,
    "max_backups": 20,
    "envs": "prod, staging, qa, analytics, dev"
}'

curl -X GET "http://localhost:8000/vela/organizations/" -H "Authorization: Bearer $TOKEN" 

curl -X POST "http://localhost:8000/vela/organizations/01K7CVGXTSC8ZT76V4G2WVJG57/projects/?response=full" \
-H "Authorization: Bearer $TOKEN" \
-H "Content-Type: application/json" \
-d '{
    "name": "MySalesTrackerApp2",
    "max_backups" : 12,
    "env_type" : "dev",
    "deployment": {
        "database": "myprojectdb",
        "database_user": "myuser",
        "database_password": "mypassword",
        "database_size": 1073741824,
        "vcpu": 1,
        "memory": 1073741824,
        "storage_size": 1073741824,
        "iops": 100,
        "database_image_tag": "15.1.0.147"
    }
}'

curl -X GET "http://localhost:8000/vela/organizations/01K7CVGXTSC8ZT76V4G2WVJG57/projects/"
curl -X DELETE "http://localhost:8000/vela/organizations/01K7CV4DWA0VC9K6HFTZRKPZ1K/projects/01K7D15M3CF6H5AHXREEX198VQ/"
curl -X GET "http://localhost:8000/vela/organizations/01K7CVGXTSC8ZT76V4G2WVJG57/projects/01K7D3Z1HRTJF1DW66X1V8TK07/branches/"
curl -X DELETE "http://localhost:8000/vela/organizations/01K7CV4DWA0VC9K6HFTZRKPZ1K/" -H "Authorization: Bearer $TOKEN"

-----------------------------------------------------------------------------------------------------------------------
DATABASE BACKUPS
-----------------------------------------------------------------------------------------------------------------------

1. TO SET THE LIMIT OF RETENTIONS, USE MAX_BACKUPS WHEN CREATING OR UPDATING AN ORGANIZATION OR PROJECT
2. BACKUP SCHEDULES CAN BE CREATED, MODIFIED AND DELETED ON THREE LEVELS: ORGANIZATION, ENVIRONMENT TYPE AND BRANCH.
   THE MOST SPECIFIC SCHEDULE IS ALWAYS USED (ENV TYPE IS MORE SPECIFIC THAN ORG AND BRANCH IS MORE 
   SPECIFIC THAN ENV TYPE).
3. WHEN MODIFYING A SCHEDULE, ALL ROWS ARE REPLACED. THE NEW SCHEDULE CAN CONTAIN MORE OR LESS ROWS.
4. NOT MORE THAN 10 ROWS ARE ALLOWED, NOT MORE THAN 59 minutes, 23 hours, 7 days and 12 weeks are allowed.
   EACH SCHEDULE ROW MUST BE DIFFERENT FROM THE OTHER ROWS, DUPLICATE ROWS NOT ACCEPTED.
5. WHEN RETRIEVING SCHEDULES FOR AN ORGANIZATION, ALL SCHEDULES FOR ALL ENV TYPES ARE RETRIEVED AS WELL.
6. THERE IS AN OPTION TO DELETE INDIVIDUAL BACKUPS AND TO CREATE AD-HOC BACKUPS. THIS MUST BE AVAILABLE IN THE UI TOO.
     
     curl -X POST "http://localhost:8000/vela/backup/organizations/01K71RXH98EP0WSCCJ169RV4QR/schedule" \   
  -H "Content-Type: application/json" \
  -d '{
        "rows": [
          {"row_index": 0, "interval": 1, "unit": "minute", "retention": 1},
          {"row_index": 1, "interval": 10, "unit": "minute", "retention": 5}]}'
          
           curl -X POST "http://localhost:8000/vela/backup/organizations/01K71RXH98EP0WSCCJ169RV4QR/schedule" \
  -H "Content-Type: application/json" \
  -d '{
        "env_type": "qa",
        "rows": [
          {"row_index": 0, "interval": 1, "unit": "minute", "retention": 2},
          {"row_index": 1, "interval": 8, "unit": "minute", "retention": 2},
          {"row_index": 2, "interval": 32, "unit": "minute", "retention": 2},
          {"row_index": 3, "interval": 4, "unit": "day", "retention": 5},
          {"row_index": 4, "interval": 1, "unit": "week", "retention": 3}
        ]
      }'

       
      curl -X POST "http://localhost:8000/vela/backup/branches/01K729MMKXJJHD1TTV1AEGCGJP/schedule" \
  -H "Content-Type: application/json" \
  -d '{
        "rows": [
          {"row_index": 0, "interval": 1, "unit": "minute", "retention": 2}
          {"row_index": 1, "interval": 15, "unit": "minute", "retention": 5}
          {"row_index": 2, "interval": 6, "unit": "hour", "retention": 8}
        ]
      }'
      
      curl -X PUT "http://localhost:8000/vela/backup/branches/01K729MMKXJJHD1TTV1AEGCGJP/schedule" \
  -H "Content-Type: application/json" \
  -d '{
        "rows": [
          {"row_index": 0, "interval": 1, "unit": "minute", "retention": 8}
        ]
      }'
             
      curl -X DELETE "http://localhost:8000/vela/backup/branches/01K729MMKXJJHD1TTV1AEGCGJP/schedule"
      curl -X DELETE "http://localhost:8000/vela/backup/organization/01K71RXH98EP0WSCCJ169RV4QR/schedule"
      curl -X POST "http://localhost:8000/vela/backup/branches/01K729MMKXJJHD1TTV1AEGCGJP/"
      curl -X DELETE "http://localhost:8000/vela/backup/01K75BQYSFG6AF2C39PW7DTSVJ/"
      


-----------------------------------------------------------------------------------------------------------------------
RESOURCE LIMITS AND CONSUMPTION
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------

1. THERE ARE MULTIPLE ADDITIONAL SETTINGS ON BOTH ORGANIZATION AND PROJECT. PROJECT IS OPTIONAL AND "OVERRIDES" ORG AS ITS MORE SPECIFIC.
   PROVISIONING LIMITS: TOTAL (ACCUMULATIVE PER ORG OR PROJECT ACROSS ALL BRANCHES) AND PER BRANCH. THESE ARE TWO DIFF. SETTINGS.
   CONSUMPTION LIMITS: MINUTES OF CONSUMPTION ALLOWED IN A CERTAIN PERIOD. THESE LIMITS CAN BE SET OR MODIFIED, BUT THEY ARE 
   NOT CHECKED.
2. LIMITS ARE SET PER RESOURCE TYPE (vcpu called milli_vcpu, ram, storage_size, iops, database_size). 
   ITS IMPORTANT TO BE CAREFUL WITH INTERNAL VS. DISPLAYED (TO THE USER) UNITS and DISCRETE STEPS BY WHICH THEY CAN BE CHANGED
   VCPU (seen by user) - INTERNALLY, EACH VCPU consists of 1000 units (milli_vcpu), BUT USERS SEE VCPU AND CAN CHANGE IN UNITS OF 0.1 
                         (100 INTERNALLY)
   RAM - INTERNALLY IN 1MB UNITS, TO THE USER in GB AND CAN BE CHANGED IN UNITS OF 0.1
   STORAGE_SIZE, DATABASE_SIZE: INTERNALLY IN 1GB UNITS, SAME AS TO USER AND CAN BE CHANGED IN UNITS OF 1
   IOPS: INTERNALLY AND TO USER IN 1, BUT CAN BE CHANGED BY USER ONLY IN STEPS OF 100.
3. I HAVE DROPPED THE IDEA OF BILLING CYCLE FOR NOW. USERS CAN SIMPLY SELECT A VARIABLE DATA/TIME RANGE TO GET THEIR METERING DATA FOR.
4. WHEN CREATING AN ORGANIZATION, ALL LIMITS MUST BE SET. IN ORGANIZATION SETTINGS, WE SHALL BE ABLE TO CHANGE THEM.
5. WHEN CREATING A PROJECT, THOSE LIMITS CAN BE SET. IN PROJECT SETTINGS, WE SHALL BE ABLE TO CHANGE THEM.


curl -X POST "http://localhost:8000/vela/resources/limits/provisioning/org/01K71RXH98EP0WSCCJ169RV4QR" \
       -H "Content-Type: application/json" \
     -d ' 
{
  "resource": "ram",
  "max_total": 1000000,
  "max_per_branch": 32000
}'
curl -X POST "http://localhost:8000/vela/resources/limits/provisioning/org/01K71RXH98EP0WSCCJ169RV4QR" \
       -H "Content-Type: application/json" \
     -d ' 
{
  "resource": "database_size",
  "max_total": 5000000,
  "max_per_branch": 250000
}'
curl -X POST "http://localhost:8000/vela/resources/limits/provisioning/org/01K71RXH98EP0WSCCJ169RV4QR" \
       -H "Content-Type: application/json" \
     -d ' 
{
  "resource": "storage_size",
  "max_total": 5000000,
  "max_per_branch": 250000
}'
curl -X POST "http://localhost:8000/vela/resources/limits/provisioning/org/01K71RXH98EP0WSCCJ169RV4QR" \
       -H "Content-Type: application/json" \
     -d ' 
{
  "resource": "iops",
  "max_total": 500000,
  "max_per_branch": 15000
}'
curl -X POST "http://localhost:8000/vela/resources/limits/provisioning/project/01K729MMKNBPMRG638ZF8X3VSF" \
       -H "Content-Type: application/json" \
     -d ' 
{
  "resource": "milli_vcpu",
  "max_total": 20000,
  "max_per_branch": 2000
}'
curl -X GET "http://localhost:8000/vela/resources/limits/provisioning/org/01K71RXH98EP0WSCCJ169RV4QR"
curl -X GET "http://localhost:8000/vela/resources/limits/provisioning/project/01K729MMKNBPMRG638ZF8X3VSF"


curl -X POST "http://localhost:8000/vela/resources/limits/consumption/org/01K71RXH98EP0WSCCJ169RV4QR"\
 -H "Content-Type: application/json" \
     -d ' 
{
  "resource": "milli_vcpu",
  "max_total_minutes": 1200000
}'

curl -X POST "http://localhost:8000/vela/resources/limits/consumption/org/01K71RXH98EP0WSCCJ169RV4QR"\
 -H "Content-Type: application/json" \
     -d ' 
{
  "resource": "ram",
  "max_total_minutes": 3000000
}'
curl -X POST "http://localhost:8000/vela/resources/limits/consumption/org/01K71RXH98EP0WSCCJ169RV4QR"\
 -H "Content-Type: application/json" \
     -d ' 
{
  "resource": "database_size",
  "max_total_minutes": 300000
}'
curl -X POST "http://localhost:8000/vela/resources/limits/consumption/org/01K71RXH98EP0WSCCJ169RV4QR"\
 -H "Content-Type: application/json" \
     -d ' 
{
  "resource": "storage_size",
  "max_total_minutes": 3000000
}'
curl -X POST "http://localhost:8000/vela/resources/limits/consumption/org/01K71RXH98EP0WSCCJ169RV4QR"\
 -H "Content-Type: application/json" \
     -d ' 
{
  "resource": "iops",
  "max_total_minutes": 300000000
}'
curl -X POST "http://localhost:8000/vela/resources/limits/consumption/project/01K729MMKNBPMRG638ZF8X3VSF"\
 -H "Content-Type: application/json" \
     -d ' 
{
  "resource": "iops",
  "max_total_minutes": 40000000
}'

curl -X GET "http://localhost:8000/vela/resources/limits/consumption/org/01K71RXH98EP0WSCCJ169RV4QR"
curl -X GET "http://localhost:8000/vela/resources/limits/consumption/project/01K729MMKNBPMRG638ZF8X3VSF"

curl -X POST "http://localhost:8000/vela/resources/branches/01K729MMKXJJHD1TTV1AEGCGJP/provision" \
       -H "Content-Type: application/json" \
     -d ' {
  "ressources": {
    "milli_vcpu": 1000,
    "ram": 5000,
    "iops": 1000,
    "storage_size": 100,
    "database_size": 10
  }
}'

----------------------------------------------------------------
RBAC
----------------------------------------------------------------
1. after deployment, before rbac can be used, fill the accessright table by running ar.sql (in the src/api/models).

curl -X GET "http://localhost:8000/vela/roles/organizations/01K7CVGXTSC8ZT76V4G2WVJG57/role-assignments?user_id=c92fdc65-ab01-4ff4-8f67-68c608973c18" \
-H "Content-Type: application/json"

curl -X GET "http://localhost:8000/vela/roles/organizations/01K7CVGXTSC8ZT76V4G2WVJG57/roles/" 

curl -X GET "http://localhost:8000/vela/roles/organizations/01K7CVGXTSC8ZT76V4G2WVJG57/role-assignments/" 

curl -X GET "http://localhost:8000/vela/roles/access-rights/" 

# Role 1: project_admin
curl -X POST "http://localhost:8000/vela/roles/organizations/01K7CVGXTSC8ZT76V4G2WVJG57/" \
-H "Content-Type: application/json" \
-d '{
  "role_type": "project",
  "is_active": true,
  "access_rights": [
    "project:settings:read",
    "project:branches:read",
    "project:branches:create"
  ]
}'

curl -X DELETE "http://localhost:8000/vela/roles/organizations/01K7CVGXTSC8ZT76V4G2WVJG57/01K7FBS4CJZ5HR4AVR1N5C8HQ6/"

# Role 2: org_admin
curl -X POST "http://localhost:8000/vela/roles/organizations/01K7CVGXTSC8ZT76V4G2WVJG57/" \
-H "Content-Type: application/json" \
-d '{
  "role_type": "organization",
  "is_active": true,
  "access_rights": [
    "org:settings:read",
    "org:settings:admin",
    "org:auth:read",
    "org:auth:admin",
    "org:backup:read",
    "org:backup:admin",
    "org:metering:read",
    "org:user:admin"
  ]
}'

# Role 3: branch_operator
curl -X POST "http://localhost:8000/vela/roles/organizations/01K7CVGXTSC8ZT76V4G2WVJG57/" \
-H "Content-Type: application/json" \
-d '{
  "role_type": "branch",
  "is_active": true,
  "access_rights": [
    "branch:auth:read",
    "branch:auth:admin",
    "branch:replicate:read",
    "branch:replicate:admin",
    "branch:import:read",
    "branch:rt:admin"
  ]
}'

# Role 4: org_manager
curl -X POST "http://localhost:8000/vela/roles/organizations/01K7CVGXTSC8ZT76V4G2WVJG57/" \
-H "Content-Type: application/json" \
-d '{
  "role_type": "organization",
  "is_active": true,
  "access_rights": [
    "org:user:read",
    "org:user:admin",
    "org:role:read",
    "org:role:admin"
  ]
}'

# Role 5: env admin
curl -X POST "http://localhost:8000/vela/roles/organizations/01K7CVGXTSC8ZT76V4G2WVJG57/" \
-H "Content-Type: application/json" \
-d '{
  "role_type": "environment",
  "is_active": true,
  "access_rights": [
    "env:backup:read",
    "env:projects:read",
    "env:projects:create"
  ]
}'

# Role 2: change org_admin
curl -X PUT "http://localhost:8000/vela/roles/organizations/01K7CVGXTSC8ZT76V4G2WVJG57/01K7FDS71KSQWZ1VKRAQARFB51/" \
-H "Content-Type: application/json" \
-d '{
  "role_type": "organization",
  "is_active": true,
  "access_rights": [
    "org:settings:read",
    "org:settings:admin",
    "org:auth:read",
    "org:auth:admin",
    "org:backup:read"
  ]
}'

curl -X POST "http://localhost:8000/vela/roles/organizations/01K7CVGXTSC8ZT76V4G2WVJG57/01K7FE330RZ26PNV5V0PDVFV12/assign/6f71a87b-43d4-4a7f-b58f-5a893da2eb9e/" \
-H "Content-Type: application/json" \
-d '{
  "project_ids": ["01K7D3XMSVZY7AY71CX50CBWXV", "01K7D3Y9Z6Z9JPRESQMB1R57ME"]
}'

curl -X POST "http://localhost:8000/vela/roles/organizations/01K7CVGXTSC8ZT76V4G2WVJG57/01K7FE81DED7QP7MAEY44SJ2HZ/assign/b3b7b2f1-bb0a-4e29-932f-8bb1d17a5b21/" \
-H "Content-Type: application/json" \
-d '{
  "environment_ids": ["dev", "qa"]
}'

curl -X POST "http://localhost:8000/vela/roles/organizations/01K7CVGXTSC8ZT76V4G2WVJG57/01K7FDZCD48PK32MCXP9XWCSYN/assign/c4a2b899-f8f3-4b2e-a4c5-472bc87fa619/" \
-H "Content-Type: application/json" \
-d '{
  "branch_ids": ["01K7D3Z1JBGX93TWHDR7B0ZF90", "01K7D3Y9ZVKYEPE6AX2K237BM2"]
}'

curl -X POST "http://localhost:8000/vela/roles/organizations/01K7CVGXTSC8ZT76V4G2WVJG57/01K7FE4NNN3R5DN7FEF6484ZSQ/assign/f1e34b9a-cb1c-4ad2-8d53-0b3e29a6b8c4/" \
-H "Content-Type: application/json" \
-d '{
}'

curl -X GET "http://localhost:8000/vela/roles/organizations/01K7CVGXTSC8ZT76V4G2WVJG57/role-assignments/"

curl -X POST "http://localhost:8000/vela/roles/organizations/01K7CVGXTSC8ZT76V4G2WVJG57/01K7FDZCD48PK32MCXP9XWCSYN/unassign/c4a2b899-f8f3-4b2e-a4c5-472bc87fa619/"