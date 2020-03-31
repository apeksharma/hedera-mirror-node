package com.hedera.mirror.importer.parser.record.transactionhandler;

import com.hederahashgraph.api.proto.java.AccountID;
import com.hederahashgraph.api.proto.java.ContractID;
import com.hederahashgraph.api.proto.java.FileID;
import com.hederahashgraph.api.proto.java.TopicID;
import javax.inject.Named;
import lombok.AllArgsConstructor;

import com.hedera.mirror.importer.domain.Entities;
import com.hedera.mirror.importer.domain.EntityId;
import com.hedera.mirror.importer.domain.EntityType;
import com.hedera.mirror.importer.repository.EntityRepository;
import com.hedera.mirror.importer.repository.EntityTypeRepository;

/**
 * TODO: Document no reliance on Entity repo. EntityId.id is always zero. no need to call hasFoo() before calling these
 * fns, if an entity is not set, it's entityNum will be 0, in which case these functions return null as entityId.
 */
@Named
@AllArgsConstructor
public class EntityHelper {
    private final EntityTypeRepository entityTypeRepository;
    private final EntityRepository entityRepository;

    EntityId create(AccountID accountID) {
        return create(accountID.getShardNum(), accountID.getRealmNum(), accountID.getAccountNum(), "account");
    }

    EntityId create(ContractID cid) {
        return create(cid.getShardNum(), cid.getRealmNum(), cid.getContractNum(), "contract");
    }

    EntityId create(FileID fileId) {
        return create(fileId.getShardNum(), fileId.getRealmNum(), fileId.getFileNum(), "file");
    }

    EntityId create(TopicID topicId) {
        return create(topicId.getShardNum(), topicId.getRealmNum(), topicId.getTopicNum(), "topic");
    }

    EntityId create(long shardNum, long realmNum, long entityNum, String type) {
        if (entityNum == 0) {
            return null;
        }
        return new EntityId(0L, shardNum, realmNum, entityNum,
                entityTypeRepository.findByName(type).map(EntityType::getId).get());
    }

    /**
     * @param entityId containing shard, realm, num, and type for which the id needs to be looked up (from cache/repo).
     *                 If no id is found, the the entity is inserted into the repo and the newly minted id is returned.
     * @return looked up/newly minted id of the given entityId.
     */
    public long lookupOrCreateId(EntityId entityId) {
        if (entityId.getId() != 0) {
            return entityId.getId();
        }
        return entityRepository.findEntityIdByNativeIds(
                entityId.getEntityShard(), entityId.getEntityRealm(), entityId.getEntityNum()).orElseGet(() -> {
            Entities entity = new Entities();
            entity.setEntityShard(entityId.getEntityShard());
            entity.setEntityRealm(entityId.getEntityRealm());
            entity.setEntityNum(entityId.getEntityNum());
            entity.setEntityTypeId(entityId.getEntityTypeId());
            return entityRepository.saveAndCacheEntityId(entity);
        }).getId();
    }

    public Entities getEntity(AccountID accountID) {
        return getEntity(accountID.getShardNum(), accountID.getRealmNum(), accountID.getAccountNum(), "account");
    }

    public Entities getEntity(ContractID cid) {
        return getEntity(cid.getShardNum(), cid.getRealmNum(), cid.getContractNum(), "contract");
    }

    public Entities getEntity(FileID fileId) {
        return getEntity(fileId.getShardNum(), fileId.getRealmNum(), fileId.getFileNum(), "file");
    }

    public Entities getEntity(TopicID topicId) {
        return getEntity(topicId.getShardNum(), topicId.getRealmNum(), topicId.getTopicNum(), "topic");
    }

    private Entities getEntity(long shardNum, long realmNum, long entityNum, String type) {
        Entities entity = new Entities();
        entity.setEntityNum(entityNum);
        entity.setEntityRealm(realmNum);
        entity.setEntityShard(shardNum);
        entity.setEntityTypeId(entityTypeRepository.findByName(type).map(EntityType::getId).get());
        return entity;
    }
}
