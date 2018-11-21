using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace OmniaMigrationTool.Queries
{
    internal static class SourceQueries
    {
        private static string[] SystemAttributes = new string[]
        {
            "Code",
            "Name"
        };

        private const string EntityQueryTemplate =
        @"SELECT * FROM [{0}].[MisEntities] me
          INNER JOIN [{0}].[MisEntities_{1}] a on me.ID = a.ID
          LEFT JOIN (SELECT * FROM (
                        SELECT av.MisEntityID, ak.Name, coalesce(foreignme.code, av.VALUE) AS Code
                        FROM [{0}].AttributeKeys ak
                        INNER JOIN [{0}].MisEntityTypes t ON ak.MisEntityTypeID = t.ID
                        INNER JOIN [{0}].vwAttributeValues av ON ak.ID = av.AttributeKeyID
                        LEFT JOIN [{0}].RelationalRules rr ON ak.ID = rr.PKID
                        LEFT JOIN [{0}].RelationalRuleInstances rri on rr.ID = rri.RelationalRuleID and rri.PKID = av.id
                        LEFT JOIN [{0}].[MisEntities] foreignme on rri.FKID = foreignme.ID
                        WHERE t.Code = @code
                        ) AS p
            PIVOT (MIN([Code]) FOR [Name] IN ({2})) as pvt
        ) AS eav ON eav.MisEntityID = me.ID";

        public static string EntityQuery(Guid tenant, string kind, string[] customAttributes)
            => string.Format(EntityQueryTemplate, tenant, kind, string.Join(",", customAttributes.Where(c=> !SystemAttributes.Contains(c))));


    }
}
