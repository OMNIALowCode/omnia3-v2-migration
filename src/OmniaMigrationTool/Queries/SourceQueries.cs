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
            "ProviderAgentCode",
            "ReceiverAgentCode",
            "ResourceCode"
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
            @"SELECT pa.Code 'ProviderAgent', ra.Code 'ReceiverAgent', rc.Code 'Resource', me.* , a.*, c.*, eav.*
          FROM [{0}].[MisEntities] me
          INNER JOIN [{0}].[MisEntities_TransactionalEntity] a on me.ID = a.ID
          INNER JOIN [{0}].[MisEntities_{1}] c on me.ID = c.ID
          INNER JOIN [{0}].[MisEntities] pa on a.ProviderAgentID = pa.ID
          INNER JOIN [{0}].[MisEntities] ra on a.ReceiverAgentID = ra.ID
          INNER JOIN [{0}].[MisEntities] rc on a.ResourceID = rc.ID
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

        private const string NumeratorsQueryTemplate = @"SELECT mt.Code 'TypeCode', comp.Code 'CompanyCode',
        num.ShortCode,
        num.LastUsedValue
    FROM [{0}].Numerators num
    INNER JOIN [{0}].MisEntityTypes mt
        ON num.MisEntityTypeID = mt.ID
    INNER JOIN [{0}].MisEntities comp
        ON num.CompanyID = comp.ID;";

        private const string CardinalityQueryTemplate = @"SELECT ak.Name, av.MisEntityID, foreignme.[Code]
        FROM [{0}].RelationalRules rr
        INNER JOIN [{0}].RelationalRuleInstances rri on rr.ID = rri.RelationalRuleID
        INNER JOIN [{0}].AttributeKeys ak ON rr.PKID = ak.ID
        INNER JOIN [{0}].MisEntityTypes t ON ak.MisEntityTypeID = t.ID
        INNER JOIN [{0}].[MisEntities] foreignme on rri.FKID = foreignme.ID
        INNER JOIN [{0}].vwAttributeValues av ON ak.ID = av.AttributeKeyID AND av.ID = rri.PKID
        WHERE Cardinality <> '1' AND t.Code = @code;";

        private const string ApprovalTrailQueryTemplate = @"SELECT fs.Code AS 'FromCode', ts.Code AS 'ToCode', u.Email, at.Note, at.[DateTime] AS 'Date', at.MisEntityID AS 'ParentID'
        FROM [{0}].ApprovalTrails at
        LEFT JOIN [{0}].ApprovalStages fs ON fs.ID = at.FromApprovalStageID
        LEFT JOIN [{0}].ApprovalStages ts ON ts.ID = at.ToApprovalStageID
        INNER JOIN [{0}].[Users] u ON u.ID = at.UserID
        LEFT JOIN [{0}].InteractionTypeInProcessTypes itpt on at.InteractionTypeInProcessTypeID = itpt.ID
        LEFT JOIN [{0}].MisEntityTypes it on it.ID = itpt.InteractionTypeID
        LEFT JOIN [{0}].MisEntityTypes mt on at.MisEntityTypeID = mt.ID
        WHERE COALESCE(mt.Code, it.Code) = @code;";

        public static string EntityQuery(Guid tenant, string kind, string[] customAttributes)
        {
            var customJoin = string.Empty;
            var customSelect = string.Empty;
            var attributesFromEav = customAttributes.Where(c => !EntitySystemAttributes.Contains(c))
                .ToArray();

            if (kind.EqualsIgnoringCase("Interaction"))
            {
                customJoin =
                    $"INNER JOIN [{tenant}].MisEntities mcomp on a.CompanyID = mcomp.ID ";
                customSelect =
                    ", mcomp.Code 'CompanyCode' ";

                attributesFromEav = attributesFromEav.Where(c => !c.Equals("CompanyCode")).ToArray();
            }
            else if (kind.EqualsIgnoringCase("Agent"))
            {
                customJoin =
                    $"LEFT JOIN [{tenant}].Users us on a.UserID = us.ID ";
                customSelect =
                    ", us.Email 'UserEmail', us.ContactEmail 'UserContactEmail' ";

                attributesFromEav = attributesFromEav.Where(c => !c.Equals("UserEmail") && !c.Equals("UserContactEmail")).ToArray();
            }

            if (!(kind.EqualsIgnoringCase("MisEntityItem") || kind.EqualsIgnoringCase("Commitment") || kind.EqualsIgnoringCase("Event")))
            {
                customJoin +=
                    $"LEFT JOIN [{tenant}].ApprovalStages aps on a.ApprovalStageID = aps.ID ";
                customSelect +=
                    ", aps.Code 'ApprovalStageCode' ";

                attributesFromEav = attributesFromEav.Where(c => !c.Equals("ApprovalStageCode")).Distinct().ToArray();
            }

            return string.Format(EntityQueryTemplate, tenant, kind,
            string.Join(",", attributesFromEav), customJoin, customSelect);
        }

        public static string TransactionalEntityQuery(Guid tenant, string kind, string[] customAttributes)
            => string.Format(TransactionalEntityQueryTemplate, tenant, kind, string.Join(",",
                customAttributes.Where(c => !EntitySystemAttributes.Contains(c) && !TransactionalEntitySystemAttributes.Contains(c)).Distinct()));

        public static string NumeratorsQuery(Guid tenant)
            => string.Format(NumeratorsQueryTemplate, tenant);

        public static string CardinalityQuery(Guid tenant)
            => string.Format(CardinalityQueryTemplate, tenant);

        public static string ApprovalTrailQuery(Guid tenant)
            => string.Format(ApprovalTrailQueryTemplate, tenant);

        public static string UsersInRolesQuery(Guid tenant)
        {
            return $@"
                SELECT users.Email, roles.Code as DomainRole  FROM [{tenant}].UsersInDomainRoles usersInRoles
                INNER JOIN[{tenant}].Users users
                    ON usersInRoles.UserID = users.ID
                INNER JOIN[{tenant}].DomainRoles roles
                    ON usersInRoles.DomainRoleID = roles.ID
                INNER JOIN [Auth].TenantUsers tu ON users.ID = tu.UserID AND tu.Status = 1
                INNER JOIN [Auth].Tenants t ON tu.TenantID = t.ID AND t.Code = '{tenant}'
                INNER JOIN [{tenant}].MisEntities_Agent a ON tu.UserID = a.UserID
                INNER JOIN [{tenant}].MisEntities e ON a.ID = e.ID AND e.Inactive = 0
                WHERE Email NOT LIKE '%@connector%' AND Email<> 'master@mymis.biz' ";
        }
    }
}