using System;

namespace OmniaMigrationTool.Queries
{
    public static class TemplateQueries
    {
        private const string EntitiesQueryTemplate = @"SELECT mt.CODE 'TypeCode',
        iif(mta.ID is null,
            iif(mtr.ID is null,
                iif(mtu.ID is null,
                    iif(mti.ID is null,
                            NULL,
                    'Interaction'),
                'UserDefinedEntity'),
            'Resource'),
        'Agent')
        'Kind'
        FROM [{0}].MisEntityTypes mt
        LEFT JOIN [{0}].[ExternalEntityTypes] et
            on mt.ExternalEntityTypeID = et.ID
        LEFT JOIN [{0}].[MisEntityTypes_ProcessType] pt
            on mt.ID = pt.ID
        LEFT JOIN [{0}].MisEntityTypes_AgentType mta
            on mt.ID = mta.ID
        LEFT JOIN [{0}].MisEntityTypes_ResourceType mtr
            on mt.ID = mtr.ID
        LEFT JOIN [{0}].MisEntityTypes_UserDefinedEntityType mtu
            on mt.ID = mtu.ID
        LEFT JOIN [{0}].MisEntityTypes_CommitmentType mtc
            on mt.ID = mtc.ID
        LEFT JOIN [{0}].MisEntityTypes_EventType mte
            on mt.ID = mte.ID
        LEFT JOIN [{0}].MisEntityTypes_InteractionType mti
            on mt.ID = mti.ID
        LEFT JOIN [{0}].MisEntityTypes_MisEntityItemType mtitem
            on mt.ID = mtitem.ID
        WHERE et.ID IS NULL AND pt.ID IS NULL AND mtitem.ID IS NULL AND mte.ID IS NULL AND mtc.ID IS NULL;";

        private const string AttributesQueryTemplate = @"SELECT mt.CODE 'TypeCode',
            ue.Code,
            ue.Name,
            ue.DataType,
            ue.Max,
            rr.Cardinality
        FROM [{0}].MisEntityTypes mt
        LEFT JOIN [{0}].[ExternalEntityTypes] et
            on mt.ExternalEntityTypeID = et.ID
        INNER JOIN [{0}].UIElements ue on mt.ID = ue.MisEntityTypeID
        LEFT JOIN [{0}].AttributeKeys ak ON ue.AttributeKeyID = ak.ID
        LEFT JOIN [{0}].RelationalRules rr ON rr.PKID = ak.ID
        WHERE et.ID IS NULL AND LEFT(ue.DataType, 2) <> 'BT' AND LEFT(ue.DataType, 2) <> 'WC'";

        public static string EntityQuery(Guid tenant)
            => string.Format(EntitiesQueryTemplate, tenant);

        public static string AttributesQuery(Guid tenant)
            => string.Format(AttributesQueryTemplate, tenant);
    }
}