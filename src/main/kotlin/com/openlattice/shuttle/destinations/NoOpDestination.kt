package com.openlattice.shuttle.destinations

import com.openlattice.data.EntityKey
import com.openlattice.data.UpdateType
import com.openlattice.data.integration.Association
import com.openlattice.data.integration.Entity
import java.util.*

class NoOpDestination : IntegrationDestination {
    override fun integrateEntities(data: Collection<Entity>, entityKeyIds: Map<EntityKey, UUID>, updateTypes: Map<UUID, UpdateType>): Long {
        return 0
    }

    override fun integrateAssociations(data: Collection<Association>, entityKeyIds: Map<EntityKey, UUID>, updateTypes: Map<UUID, UpdateType>): Long {
        return 0
    }

    override fun accepts(): StorageDestination {
        return StorageDestination.NO_OP
    }
}