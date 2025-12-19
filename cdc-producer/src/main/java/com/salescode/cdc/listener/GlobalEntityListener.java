package com.salescode.cdc.listener;

import org.hibernate.event.spi.*;
import org.hibernate.persister.entity.EntityPersister;

import javax.persistence.PostPersist;
import javax.persistence.PostRemove;
import javax.persistence.PostUpdate;

public class GlobalEntityListener implements PostInsertEventListener,
        PostUpdateEventListener,
        PostDeleteEventListener {
    @Override
    public void onPostDelete(PostDeleteEvent postDeleteEvent) {

    }

    @Override
    public void onPostInsert(PostInsertEvent postInsertEvent) {

    }

    @Override
    public void onPostUpdate(PostUpdateEvent postUpdateEvent) {

    }

    @Override
    public boolean requiresPostCommitHanding(EntityPersister entityPersister) {
        return false;
    }
}
