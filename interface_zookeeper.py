from ops.framework import EventBase, EventsBase, EventSource, Object, StoredState
from ops.model import BlockedStatus, ModelError, WaitingStatus


class ZookeeperError(ModelError):
    status_type = BlockedStatus
    status_message = 'Zookeeper error'

    def __init__(self, relation_name):
        super().__init__(relation_name)
        self.status = self.status_type(f'{self.status_message}: {relation_name}')


class IncompleteRelationError(ZookeeperError):
    status_type = WaitingStatus
    status_message = 'Waiting for relation'


class NoRelatedAppsError(ZookeeperError):
    status_message = 'Missing relation'


class TooManyRelatedAppsError(ZookeeperError):
    status_message = 'Too many related applications'


class ZookeeperEvent(EventBase):
    def __init__(self, handle, zookeeper):
        super().__init__(handle)
        self.zookeeper = zookeeper
    
    def snapshot(self):
        return dict(self.zookeeper)
    
    def restore(self, snapshot):
        self.zookeeper = Zookeeper(snapshot)


class ZookeperAvailableEvent(ZookeeperEvent):
    pass


class ZookeeperChangedEvent(ZookeeperEvent):
    pass


class ZookeeperLostEvent(EventBase):
    def __init__(self, handle, relation_name):
        super().__init__(handle)
        self.relation_name = relation_name
        self.status = NoRelatedAppsError(relation_name).status

    def snapshot(self):
        return self.relation_name

    def restore(self, relation_name):
        self.relation_name = relation_name
        self.status = NoRelatedAppsError(relation_name).status


class ZookeeperClientEvents(EventsBase):
    zookeeper_available = EventSource(ZookeperAvailableEvent)
    zookeeper_changed = EventSource(ZookeeperChangedEvent)
    zookeeper_lost = EventSource(ZookeeperLostEvent)


class ZookeeperClient(Object):
    on = ZookeeperClientEvents()
    state = StoredState()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self.state.set_default(zookeeper_hash=None)
        self.relation_name = relation_name

        self.framework.observe(charm.on[relation_name].relation_changed, self.on_changed)
        self.framework.observe(charm.on[relation_name].relation_broken, self.on_broken)

    def on_changed(self, event):
        try:
            zookeeper = self.zookeeper()
        except ModelError as e:
            had_zookeper = self.state.zookeeper_hash is not None
            self.state.zookeeper_hash = None
            if had_zookeper:
                self.on.zookeeper_lost.emit(e.status)
        else:
            old_hash = self.state.zookeeper_hash
            new_hash = self.state.zookeeper_hash = hash(frozenset(zookeeper.items()))
            if old_hash is None:
                self.on.zookeeper_available.emit(zookeeper)
            elif new_hash != old_hash:
                self.on.zookeeper_changed.emit(zookeeper)
    
    def on_broken(self, event):
        self.state.zookeeper_hash = None
        self.on.zookeeper_lost.emit(event.relation.name)
    
    @property
    def _relations(self):
        return self.framework.model.relations[self.relation_name]
    
    def zookeeper(self):
        if not self._relations:
            raise NoRelatedAppsError(self.relation_name)
        if len(self._relations) > 1:
            raise TooManyRelatedAppsError(self.relation_name)
        zookeeper = Zookeeper.from_relation(self._relations[0])
        if zookeeper is None:
            raise IncompleteRelationError(self.relation_name)
        return zookeeper


class Zookeeper(dict):
    @classmethod
    def from_relation(cls, relation):
        for candidate in [relation.app] + list(relation.units):
            data = relation.data[candidate]
            if all(data.get(field) for field in ('host', 'port', 'rest_port')):
                return cls(
                    host=data['host'],
                    port=data['port'],
                    rest_port=data['rest_port']
                )
        else:
            return None
    
    @property
    def host(self):
        return self['host']
    
    @property
    def port(self):
        return self['port']
    
    @property
    def rest_port(self):
        return self['rest_port']
