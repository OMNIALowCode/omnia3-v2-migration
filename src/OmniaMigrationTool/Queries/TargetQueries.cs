namespace OmniaMigrationTool.Queries
{
    internal static class TargetQueries
    {
        internal const string TenantSchemaQuery = @"SELECT '_' || replace(e.id::text, '-','') || '_business'
            FROM tenants.directory t
            INNER JOIN tenants.environments e on t.id = e.tenant_id
            WHERE t.code = @code AND e.code = 'PRD'";
    }
}