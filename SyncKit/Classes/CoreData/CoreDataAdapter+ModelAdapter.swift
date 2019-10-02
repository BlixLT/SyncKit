//
//  CoreDataAdapter+ModelAdapter.swift
//  SyncKit
//
//  Created by Manuel Entrena on 04/06/2019.
//  Copyright © 2019 Manuel Entrena. All rights reserved.
//

import Foundation
import CoreData
import CloudKit

extension CoreDataAdapter: ModelAdapter {

    public func prepareToImport() {
        configureImportContext()
        privateContext.performAndWait {
            self.deleteAllPendingRelationships()
            self.deleteInsertedButUnmergedEntities()
            self.savePrivateContext()
        }
    }

    public func saveChanges(in records: [CKRecord], completion: @escaping (Error?)->()) {
        guard records.count > 0 else {
            completion(nil)
            return
        }
        
        privateContext.perform {
            debugPrint("Save changes in records")
            let identifiers = records.map { $0.recordID.recordName }
            let syncedEntities = self.fetchEntities(identifiers: identifiers)
            var entitiesById = syncedEntities.reduce(into: [String: QSSyncedEntity]()) { dict, entity in
                dict[entity.identifier] = entity
            }
            
            var queryByEntityType = [String: [String: QueryData]]()
            for record in records {
                var syncedEntity: QSSyncedEntity! = entitiesById[record.recordID.recordName]
                if syncedEntity == nil {
                    if #available(iOS 10.0, *),
                        let share = record as? CKShare {
                        syncedEntity = self.createSyncedEntity(share: share)
                    } else {
                        syncedEntity = self.createSyncedEntity(record: record)
                    }
                    entitiesById[record.recordID.recordName] = syncedEntity
                }
                
                guard syncedEntity.entityState != .deleted && !syncedEntity.isShare,
                    let entityType = syncedEntity.entityType,
                    let originObjectID = syncedEntity.originObjectID else {
                    continue
                }
                
                let query = QueryData(identifier: originObjectID,
                                      record: record,
                                      entityType: entityType,
                                      changedKeys: syncedEntity.changedKeysArray,
                                      state: syncedEntity.entityState)
                if queryByEntityType[entityType] == nil {
                    queryByEntityType[entityType] = [String: QueryData]()
                }
                queryByEntityType[entityType]?[originObjectID] = query
            }
            
            var tempTargetImportContext : NSManagedObjectContext? = nil
            if self.targetImportContext == nil {
                debugPrint("save records. will configure import context")
                self.configureImportContext()
                tempTargetImportContext = self.targetImportContext
            }
            self.targetImportContext.performAndWait {
                debugPrint("Applying attribute changes in records")
                for entityType in queryByEntityType.keys {
                    var queries = queryByEntityType[entityType]!
                    var objects = self.managedObjects(entityName: entityType, identifiers: Array(queries.keys), context: self.targetImportContext)
                    
                    if objects.count < Array(queries.keys).count && self.isShared() {
                        // it is possible that some parent(s) were deleted/moved to not shared account and because of cascade deletion rule children were deleted (during context save/merging changes after import), but this deletion was not registered by synckit
                        debugPrint("identifiers.count: \(identifiers.count) - objects.count : \(objects.count) , entityType : \(entityType)")
                        let existingIdentifiers = objects.map { $0.value(forKey: self.identifierFieldName(forEntity: entityType)) as! String }
                        debugPrint(Array(queries.keys), " - ", existingIdentifiers)
                        for anIdentifier in Array(queries.keys) {
                            if !(existingIdentifiers.contains(anIdentifier))
                            {
                                debugPrint("will create missing shared object of entityType \(entityType) with identifier \(anIdentifier)")
                                let object = self.insertManagedObject(entityName: entityType)
                                object.setValue(anIdentifier, forKey: self.identifierFieldName(forEntity: entityType))
                            }
                        }
                        objects = self.managedObjects(entityName: entityType, identifiers: Array(queries.keys), context: self.targetImportContext)
                    }
                    for object in objects {
                        guard let query = queries[self.uniqueIdentifier(for: object)],
                        let record = query.record else { continue }
                        self.applyAttributeChanges(record: record,
                                                   to: object,
                                                   state: query.state,
                                                   changedKeys: query.changedKeys)
                        let relationshipsToSave = self.prepareRelationshipChanges(for: object, record: record)
                        query.toSaveRelationshipNames = relationshipsToSave
                    }
                }
                if tempTargetImportContext != nil
                {
                    debugPrint("save records. will clear import context")
                    tempTargetImportContext!.performAndWait {
                        tempTargetImportContext!.reset()
                    }
                    tempTargetImportContext = nil
                }
            }
            
            for record in records {
                guard let syncedEntity = entitiesById[record.recordID.recordName],
                    let entityType = syncedEntity.entityType else { continue }
                
                if let originObjectID = syncedEntity.originObjectID,
                    let queries = queryByEntityType[entityType],
                    let query = queries[originObjectID],
                    let relationshipsToSave = query.toSaveRelationshipNames {
                    self.saveRelationshipChanges(record: record, names: relationshipsToSave, entity: syncedEntity)
                }
                
                self.saveShareRelationship(for: syncedEntity, record: record)
                syncedEntity.updatedDate = record[CoreDataAdapter.timestampKey]
                self.save(record: record, for: syncedEntity)
            }
            completion(nil)
        }
    }

    public func saveChanges(in records: [CKRecord]) {
        self.saveChanges(in: records, completion: { (Error) in })
    }
    
    public func deleteRecords(with recordIDs: [CKRecord.ID]) {
        guard recordIDs.count > 0 else { return }
        
        privateContext.perform {
            let entities = recordIDs.compactMap { self.syncedEntity(withIdentifier: $0.recordName) }
            self.delete(syncedEntities: entities)
        }
    }
    
    public func persistImportedChanges(completion: @escaping (Error?)->()) {
        privateContext.perform {
            self.applyPendingRelationships()
            self.mergeChangesIntoTargetContext(completion: { (error) in
                if error != nil {
                    self.privateContext.reset()
                } else {
                    self.privateContext.perform {
                        self.updateInsertedEntitiesAndSave()
                    }
                }
                completion(error)
            })
        }
    }
    
    public func recordsToUpload(limit: Int) -> [CKRecord] {
        var uploadingState = SyncedEntityState.new
        var recordsArray = [CKRecord]()
        let limit = limit == 0 ? Int.max : limit
        var innerLimit = limit
        while recordsArray.count < limit && uploadingState.rawValue < SyncedEntityState.deleted.rawValue {
            recordsArray.append(contentsOf: self.recordsToUpload(state: uploadingState, limit: innerLimit))
            uploadingState = nextStateToSync(after: uploadingState)
            innerLimit = limit - recordsArray.count
        }
        return recordsArray
    }
    
    public func didUpload(savedRecords: [CKRecord]) {
        guard savedRecords.count > 0 else { return }
        privateContext.perform {
            for record in savedRecords {
                if let entity = self.syncedEntity(withIdentifier: record.recordID.recordName) {
                    if record[CoreDataAdapter.timestampKey] == entity.updatedDate {
                        entity.entityState = .synced
                        entity.changedKeysArray = []
                    }
                    self.save(record: record, for: entity)
                }
            }
            self.savePrivateContext()
        }
    }
    
    public func recordIDsMarkedForDeletion(limit: Int) -> [CKRecord.ID] {
        var recordIDs = [CKRecord.ID]()
        privateContext.performAndWait {
            let unsortedDeletedEntities = self.fetchEntities(state: .deleted)
            var deletedEntities = sortedEntities(entities:unsortedDeletedEntities)
            // upload deletions with reversed sort compared to insertions
            deletedEntities = deletedEntities.reversed()
            for entity in deletedEntities {
                let record = self.storedRecord(for: entity)
                if let record = record {
                    recordIDs.append(record.recordID)
                } else {
                    self.privateContext.delete(entity)
                }
                if recordIDs.count > limit {
                    break
                }
            }
        }
        return recordIDs
    }
    
    public func didDelete(recordIDs: [CKRecord.ID]) {
        guard recordIDs.count > 0 else { return }
        privateContext.perform {
            for recordID in recordIDs {
                if let entity = self.syncedEntity(withIdentifier: recordID.recordName) {
                    self.privateContext.delete(entity)
                }
            }
            self.savePrivateContext()
        }
    }
    
    public func hasRecordID(_ recordID: CKRecord.ID) -> Bool {
        var hasEntity = false
        privateContext.performAndWait {
            if self.syncedEntity(withIdentifier: recordID.recordName) != nil {
                hasEntity = true
            }
        }
        return hasEntity
    }
    
    public func didFinishImport(with error: Error?) {
        privateContext.performAndWait {
            self.savePrivateContext()
            self.updateHasChanges()
        }
        
        clearImportContext()
        tempFileManager.clearTempFiles()
    }
    
    public var serverChangeToken: CKServerChangeToken? {
        var token: CKServerChangeToken?
        privateContext.performAndWait {
            if let qsToken = try? self.privateContext.executeFetchRequest(entityName: "QSServerToken",
                                                                          fetchLimit: 1).first as? QSServerToken,
                let data = qsToken.token {
                token = QSCoder.shared.object(from: data as Data) as? CKServerChangeToken
            }
        }
        return token
    }
    
    public func saveToken(_ token: CKServerChangeToken?) {
        privateContext.performAndWait {
            var qsToken: QSServerToken! = try? self.privateContext.executeFetchRequest(entityName: "QSServerToken",
                                                                                       fetchLimit: 1).first as? QSServerToken
            if qsToken == nil {
                qsToken = NSEntityDescription.insertNewObject(forEntityName: "QSServerToken", into: self.privateContext) as? QSServerToken
            }
            if let token = token {
                qsToken.token = QSCoder.shared.data(from: token) as NSData?
            } else {
                qsToken.token = nil
            }
            self.savePrivateContext()
        }
    }
    
    public func deleteChangeTracking() {
        stack.deleteStore()
        privateContext = nil
        clearImportContext()
    }
    
    public func record(for object: AnyObject) -> CKRecord? {
        guard let object = object as? IdentifiableManagedObject else { return nil }
        let objectIdentifier = threadSafePrimaryKeyValue(for: object)
        var record: CKRecord?
        privateContext.performAndWait {
            if let entity = syncedEntity(withOriginIdentifier: objectIdentifier) {
                var parent: QSSyncedEntity?
                record = self.recordToUpload(for: entity, context: self.targetContext, parentEntity: &parent)
            }
        }
        return record
    }
    
    @available(iOS 10.0, OSX 10.12, *)
    public func share(for object: AnyObject) -> CKShare? {
        guard let object = object as? IdentifiableManagedObject else { return nil }
        let objectIdentifier = threadSafePrimaryKeyValue(for: object)
        var record: CKShare?
        privateContext.performAndWait {
            if let entity = syncedEntity(withOriginIdentifier: objectIdentifier) {
                record = self.storedShare(for: entity)
            }
        }
        return record
    }

    @available(iOS 10.0, OSX 10.12, *)
    public func share(for object: AnyObject, completion: @escaping (CKShare?, Error?)->()) {
        guard let object = object as? IdentifiableManagedObject else {
            completion(nil, nil)
            return
        }
        let objectIdentifier = threadSafePrimaryKeyValue(for: object)
        var record: CKShare?
        privateContext.perform {
            if let entity = self.syncedEntity(withOriginIdentifier: objectIdentifier) {
                record = self.storedShare(for: entity)
            }
            completion(record, nil)
        }
    }

    @available(iOS 10.0, OSX 10.12, *)
    public func save(share: CKShare, for object: AnyObject) {
        guard let object = object as? IdentifiableManagedObject else { return }
        let objectIdentifier = threadSafePrimaryKeyValue(for: object)
        privateContext.performAndWait {
            if let entity = syncedEntity(withOriginIdentifier: objectIdentifier) {
                self.save(share: share, for: entity)
                self.savePrivateContext()
            }
        }
    }
    
    @available(iOS 10.0, OSX 10.12, *)
    public func deleteShare(for object: AnyObject) {
        guard let object = object as? IdentifiableManagedObject else { return }
        let objectIdentifier = threadSafePrimaryKeyValue(for: object)
        privateContext.performAndWait {
            if let share = syncedEntity(withOriginIdentifier: objectIdentifier)?.share {
                if let record = share.record {
                    self.privateContext.delete(record)
                }
                self.privateContext.delete(share)
                self.savePrivateContext()
            }
        }
    }
    
    public func recordsToUpdateParentRelationshipsForRoot(_ object: AnyObject) -> [CKRecord] {
        guard let object = object as? IdentifiableManagedObject else { return [] }
        let objectIdentifier = threadSafePrimaryKeyValue(for: object)
        var records: [CKRecord]!
        privateContext.performAndWait {
            if let entity = self.syncedEntity(withOriginIdentifier: objectIdentifier) {
                records = self.childrenRecords(for: entity)
            }
        }
        return records ?? []
    }
    
    public func recordsToUpdateParentRelationshipsForRoot(_ object: AnyObject, completion: @escaping ([CKRecord])->()) -> () {
        guard let object = object as? IdentifiableManagedObject else {
            completion([])
            return
        }
        let objectIdentifier = threadSafePrimaryKeyValue(for: object)
        var records: [CKRecord]!
        privateContext.perform {
            if let entity = self.syncedEntity(withOriginIdentifier: objectIdentifier) {
                records = self.childrenRecords(for: entity)
            }
            completion(records ?? [])
        }
    }
}
