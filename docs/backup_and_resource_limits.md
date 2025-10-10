-----------------------------------------------------------------------------------------------------------------------
DATABASE BACKUPS
-----------------------------------------------------------------------------------------------------------------------

1. TO SET THE LIMIT OF RETENTIONS, USE MAX_BACKUPS WHEN CREATING OR UPDATING AN ORGANIZATION OR PROJECT
2. ENV_TYPES CAN BE SET WHEN CREATING OR UPDATING AN ORGANIZATION. IT CONTAINS A COMMA-SEPARATED LIST OF ACCEPTED
   ENVIRONMENT TYPES.
3. A SPECIFIC ENV_TYPE HAS TO BE SET WHEN CREATING A BRANCH (OR A PROJECT WITH A BRANCH).
4. BACKUP SCHEDULES CAN BE CREATED, MODIFIED AND DELETED ON THREE LEVELS: ORGANIZATION, ENVIRONMENT TYPE AND BRANCH.
   THE MOST SPECIFIC SCHEDULE IS ALWAYS USED (ENV TYPE IS MORE SPECIFIC THAN ORG AND BRANCH IS MORE 
   SPECIFIC THAN ENV TYPE).
5. WHEN MODIFYING A SCHEDULE, ALL ROWS ARE REPLACED. THE NEW SCHEDULE CAN CONTAIN MORE OR LESS ROWS.
6. NOT MORE THAN 10 ROWS ARE ALLOWED, NOT MORE THAN 59 minutes, 23 hours, 7 days and 12 weeks are allowed.
   EACH SCHEDULE ROW MUST BE DIFFERENT FROM THE OTHER ROWS, DUPLICATE ROWS NOT ACCEPTED.
7. WHEN RETRIEVING SCHEDULES FOR AN ORGANIZATION, ALL SCHEDULES FOR ALL ENV TYPES ARE RETRIEVED AS WELL.
8. THERE IS AN OPTION TO DELETE INDIVIDUAL BACKUPS AND TO CREATE AD-HOC BACKUPS. THIS MUST BE AVAILABLE IN THE UI TOO.
     
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

curl -X POST "http://localhost:8000/vela/resources/branches/01K729NDSB6PAKCG50G0XBSSQ4/provision" \
       -H "Content-Type: application/json" \
     -d ' {
  "ressources": {
    "milli_vcpu": 1500,
    "ram": 8000,
    "iops": 12500,
    "storage_size": 150,
    "database_size": 80
  }
}'

curl -X POST "http://localhost:8000/vela/resources/branches/01K74V8V254V2KSMZ4WMRZCAN0/provision" \
       -H "Content-Type: application/json" \
     -d ' {
  "ressources": {
    "milli_vcpu": 800,
    "ram": 12000,
    "iops": 5000,
    "storage_size": 300,
    "database_size": 150
  }
}'

curl -X POST "http://localhost:8000/vela/resources/branches/01K729MMKXJJHD1TTV1AEGCGJP/provision" \
       -H "Content-Type: application/json" \
     -d ' {
  "ressources": {
    "milli_vcpu": 1600,
    "ram": 16000,
    "iops": 8000,
    "storage_size": 100,
    "database_size": 10
  }
}'

curl -X GET "http://localhost:8000/vela/resources/branches/01K74V8V254V2KSMZ4WMRZCAN0/limits"

curl -X GET "http://localhost:8000/vela/resources/organizations/01K71RXH98EP0WSCCJ169RV4QR/usage"\
      -H "Content-Type: application/json" \
     -d ' {
  "cycle_start": "2025-10-10T19:00:00Z",
  "cycle_end": "2025-10-10T23:00:00Z"
}'

curl -X GET "http://localhost:8000/vela/resources/projects/01K729MMKNBPMRG638ZF8X3VSF/usage"\
      -H "Content-Type: application/json" \
     -d ' {
  "cycle_start": "2025-10-10T19:00:00Z",
  "cycle_end": "2025-10-10T23:00:00Z"
}'



      





