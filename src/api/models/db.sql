-- Create Organizations Table
CREATE TABLE organizations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255),
    max_backups INTEGER NOT NULL
);

-- Create Projects Table
CREATE TABLE projects (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE
);

-- Create Branches Table
CREATE TABLE branches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    env_type VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    max_backups INTEGER NOT NULL
);

-- Create BackupSchedules Table
CREATE TABLE backup_schedules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,
    env_type VARCHAR(50),
    branch_id UUID REFERENCES branches(id) ON DELETE CASCADE
);

-- Create BackupScheduleRows Table
CREATE TABLE backup_schedule_rows (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    schedule_id UUID REFERENCES backup_schedules(id) ON DELETE CASCADE,
    row_index INTEGER NOT NULL,
    interval INTEGER NOT NULL,
    unit VARCHAR(50) NOT NULL,
    retention INTEGER NOT NULL,
    CONSTRAINT ix_backup_schedule_row_schedule_index UNIQUE(schedule_id, row_index)
);

-- Create NextBackups Table
CREATE TABLE next_backups (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    branch_id UUID REFERENCES branches(id) ON DELETE CASCADE,
    schedule_id UUID NOT NULL,
    row_index INTEGER NOT NULL,
    next_at TIMESTAMP NOT NULL,
    CONSTRAINT ix_next_backups_branch_row UNIQUE(branch_id, row_index)
);

-- Create Backups Table
CREATE TABLE backups (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    branch_id UUID REFERENCES branches(id) ON DELETE CASCADE,
    row_index INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    size_bytes INTEGER
);

-- Create BackupLogs Table
CREATE TABLE backup_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    branch_id UUID NOT NULL,
    backup_uuid UUID NOT NULL,
    action VARCHAR(255) NOT NULL,
    ts TIMESTAMP DEFAULT NOW()
);

-- Create ResourceLimits Table
CREATE TABLE resource_limits (
    id SERIAL PRIMARY KEY,
    entity_type VARCHAR(50) NOT NULL,
    resource VARCHAR(50) NOT NULL,
    org_id UUID REFERENCES organizations(id) ON DELETE CASCADE,
    env_type VARCHAR(50),
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    max_total BIGINT NOT NULL,
    max_per_branch BIGINT NOT NULL,
    CONSTRAINT unique_resource_limits UNIQUE(entity_type, org_id, env_type, project_id, resource)
);

-- Create BranchProvisionings Table
CREATE TABLE branch_provisionings (
    id SERIAL PRIMARY KEY,
    branch_id UUID REFERENCES branches(id) ON DELETE CASCADE,
    resource VARCHAR(50) NOT NULL,
    amount BIGINT NOT NULL,
    updated_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT unique_branch_provisioning UNIQUE(branch_id, resource)
);

-- Create ProvisioningLog Table
CREATE TABLE provisioning_log (
    id SERIAL PRIMARY KEY,
    branch_id UUID REFERENCES branches(id) ON DELETE CASCADE,
    resource VARCHAR(50) NOT NULL,
    amount BIGINT NOT NULL,
    action VARCHAR(255) NOT NULL,
    reason VARCHAR(255),
    ts TIMESTAMP DEFAULT NOW()
);

-- Create ResourceUsageMinutes Table
CREATE TABLE resource_usage_minutes (
    id SERIAL PRIMARY KEY,
    ts_minute TIMESTAMP NOT NULL,
    org_id UUID REFERENCES organizations(id) ON DELETE CASCADE,
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    branch_id UUID REFERENCES branches(id) ON DELETE CASCADE,
    resource VARCHAR(50) NOT NULL,
    amount BIGINT NOT NULL,
    CONSTRAINT unique_resource_usage_minutes UNIQUE(ts_minute, org_id, project_id, branch_id, resource)
);

-- Create ResourceConsumptionLimits Table
CREATE TABLE resource_consumption_limits (
    id SERIAL PRIMARY KEY,
    entity_type VARCHAR(50) NOT NULL,
    org_id UUID REFERENCES organizations(id) ON DELETE CASCADE,
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    resource VARCHAR(50) NOT NULL,
    max_total_minutes BIGINT NOT NULL,
    CONSTRAINT unique_resource_consumption_limits UNIQUE(entity_type, org_id, project_id, resource)
);
