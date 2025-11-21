CREATE DATABASE OrgProfileDB;
CREATE STAGE my_stage_org_profile;
COPY INTO @my_stage_org_profile/manifest.yml
  FROM (
    SELECT $$
      title: "Energy_Operations"
      description: "Profile for Energy Operations"
      contact: "gael.charriere@snowflake.com"
      approver_contact: "gael.charriere@snowflake.com"
      allowed_publishers:
        access:
          - all_internal_accounts: "true"
      logo: "urn:icon:shieldlock:blue"
    $$
  )
  SINGLE = TRUE
  OVERWRITE = TRUE
  FILE_FORMAT = (
    COMPRESSION = NONE
    ESCAPE_UNENCLOSED_FIELD = NONE
  );

CREATE ORGANIZATION PROFILE OPERATIONS
 FROM @my_stage_org_profile
 PUBLISH=TRUE;