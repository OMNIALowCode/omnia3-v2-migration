using Omnia.Libraries.GenericExtensions;
using System;
using System.Linq;

namespace OmniaMigrationTool.Queries
{
    internal static class SourceQueries
    {
        private static readonly string[] EntitySystemAttributes = new[]
        {
            "Code",
            "Name"
        };

        private static readonly string[] TransactionalEntitySystemAttributes = new[]
        {
            "Quantity",
            "Amount",
            "DateOccurred",
        };

        private const string EntityQueryTemplate =
        @"SELECT me.* , a.*, eav.* {4}
          FROM [{0}].[MisEntities] me
          INNER JOIN [{0}].[MisEntities_{1}] a on me.ID = a.ID
          INNER JOIN [{0}].MisEntityTypes t ON me.MisEntityTypeID = t.ID
          {3}
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
        ) AS eav ON eav.MisEntityID = me.ID
        WHERE t.Code = @code;";

        private const string TransactionalEntityQueryTemplate =
            @"SELECT me.* , a.*, c.*, eav.*
          FROM [{0}].[MisEntities] me
          INNER JOIN [{0}].[MisEntities_TransactionalEntity] a on me.ID = a.ID
          INNER JOIN [{0}].[MisEntities_{1}] c on me.ID = c.ID
          INNER JOIN [{0}].MisEntityTypes t ON me.MisEntityTypeID = t.ID
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
        ) AS eav ON eav.MisEntityID = me.ID
        WHERE t.Code = @code;";

        public static string EntityQuery(Guid tenant, string kind, string[] customAttributes)
        {
            var customJoin = string.Empty;
            var customSelect = string.Empty;
            var attributesFromEav = customAttributes.Where(c => !EntitySystemAttributes.Contains(c))
                .ToArray();
            if (kind.EqualsIgnoringCase("Interaction"))
            {
                customJoin =
                    $"INNER JOIN [{tenant}].MisEntities mcomp on a.CompanyID = mcomp.ID";
                customSelect =
                    ", mcomp.Code 'CompanyCode'";

                attributesFromEav = attributesFromEav.Where(c => !c.Equals("CompanyCode")).ToArray();
            }

            return string.Format(EntityQueryTemplate, tenant, kind,
                string.Join(",", attributesFromEav), customJoin, customSelect);
        }

        public static string TransactionalEntityQuery(Guid tenant, string kind, string[] customAttributes)
            => string.Format(TransactionalEntityQueryTemplate, tenant, kind, string.Join(",",
                customAttributes.Where(c => !EntitySystemAttributes.Contains(c) && !TransactionalEntitySystemAttributes.Contains(c))));

    }
}
