using System;

namespace OmniaMigrationTool.Queries
{
    public static class TemplateQueries
    {
        private const string EntitiesQueryTemplate = @"SELECT mt.CODE 'TypeCode',
        iif(mta.ID is null,
            iif(mtr.ID is null,
                iif(mtu.ID is null,
                    iif(mte.ID is null,
                        iif(mtc.ID is null,
                            iif(mti.ID is null,
                                iif(mtitem.ID is null,
                                    NULL,
                                'MisEntityItem'),
                            'Interaction'),
                        'Commitment'),
                    'Event'),
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
        WHERE et.ID IS NULL AND pt.ID IS NULL;";

        private const string AttributesQueryTemplate = @"SELECT mt.CODE 'TypeCode',
            ue.Code,
            ue.Name,
            ue.DataType,
            ue.Max,
            rr.Cardinality,
            ue.Base
        FROM [{0}].MisEntityTypes mt
        LEFT JOIN [{0}].[ExternalEntityTypes] et
            on mt.ExternalEntityTypeID = et.ID
        INNER JOIN [{0}].UIElements ue on mt.ID = ue.MisEntityTypeID
        LEFT JOIN [{0}].AttributeKeys ak ON ue.AttributeKeyID = ak.ID
        LEFT JOIN [{0}].RelationalRules rr ON rr.PKID = ak.ID
        INNER JOIN  [{0}].UIElementsInViews uev on ue.ID = uev.UIElementID
        INNER JOIN  [{0}].Views vw on vw.ID = uev.ViewID AND vw.[Default] = 1
        WHERE et.ID IS NULL AND LEFT(ue.DataType, 2) <> 'BT' AND LEFT(ue.DataType, 2) <> 'WC'";

        private const string ItemsQueryTemplate = @"SELECT mt.CODE 'TypeCode', mit.Code 'ItemTypeCode'
        FROM [{0}].MisEntityTypes mt
        LEFT JOIN [{0}].[ExternalEntityTypes] et
            on mt.ExternalEntityTypeID = et.ID
        INNER JOIN [{0}].MisEntityTypes_MisEntityItemType mitem on mt.ID = mitem.MisEntityTypeID
        INNER JOIN [{0}].MisEntityTypes mit on mit.ID = mitem.ID
        WHERE et.ID is null;";

        private const string CommitmentsQueryTemplate = @"SELECT mit.CODE 'TypeCode', mt.Code 'CommitmentTypeCode'
        FROM [{0}].MisEntityTypes mt
        INNER JOIN [{0}].[MisEntities_Commitment] ct
            on mt.ID = ct.ID
        INNER JOIN [{0}].TransactionalEntityTypesInInteractionTypes teit
            on teit.TransactionalEntityTypeID = ct.ID
        INNER JOIN [{0}].MisEntityTypes mit on mit.ID = teit.InteractionTypeID;";

        private const string EventsQueryTemplate = @"SELECT mit.CODE 'TypeCode', mt.Code 'EventTypeCode'
        FROM [{0}].MisEntityTypes mt
        INNER JOIN [{0}].[MisEntities_Event] ct
            on mt.ID = ct.ID
        INNER JOIN [{0}].TransactionalEntityTypesInInteractionTypes teit
            on teit.TransactionalEntityTypeID = ct.ID
        INNER JOIN [{0}].MisEntityTypes mit on mit.ID = teit.InteractionTypeID;";

        public static string EntityQuery(Guid tenant)
            => string.Format(EntitiesQueryTemplate, tenant);

        public static string AttributesQuery(Guid tenant)
            => string.Format(AttributesQueryTemplate, tenant);

        public static string ItemsQuery(Guid tenant)
            => string.Format(ItemsQueryTemplate, tenant);

        public static string CommitmentsQuery(Guid tenant)
            => string.Format(CommitmentsQueryTemplate, tenant);

        public static string EventsQuery(Guid tenant)
            => string.Format(EventsQueryTemplate, tenant);
    }
}