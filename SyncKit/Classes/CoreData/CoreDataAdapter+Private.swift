//
//  CoreDataAdapter+Private.swift
//  SyncKit
//
//  Created by Manuel Entrena on 04/06/2019.
//  Copyright © 2019 Manuel Entrena. All rights reserved.
//

import Foundation
import CoreData
import CloudKit

typealias IdentifiableManagedObject = NSManagedObject & PrimaryKey

//MARK: - Utilities
extension CoreDataAdapter {
    func savePrivateContext() {
        debugPrint("savePrivateContext", self.recordZoneID)
        try? self.privateContext.save()
        self.saveImportContext()
    }
    
    func saveImportContext()
    {
        if let contextToSave = self.targetImportContext {
            debugPrint("saveImportContext")
            var saveError: Error?
            contextToSave.performAndWait {
                do {
                    try contextToSave.save()
                } catch {
                    saveError = error
                    debugPrint(saveError)
                }
            }
        }
        else
        {
            debugPrint("saveImportContext. no context")
        }
    }
    
    func configureImportContext() {
        debugPrint("configureImportContext", self.recordZoneID);
        targetImportContext = NSManagedObjectContext(concurrencyType: .privateQueueConcurrencyType)
        targetImportContext.mergePolicy = NSMergePolicy(merge: NSMergePolicyType.mergeByPropertyObjectTrumpMergePolicyType)
        targetImportContext.persistentStoreCoordinator = targetContext.persistentStoreCoordinator;
        NotificationCenter.default.addObserver(self,
                                               selector: #selector(targetImportContextDidSave(notification:)),
                                               name: .NSManagedObjectContextDidSave,
                                               object: targetImportContext)

    }
    
    func clearImportContext() {
        debugPrint("clearImportContext", self.recordZoneID);
        guard let targetImportContext = targetImportContext else { return }
        targetImportContext.performAndWait {
            self.targetImportContext.reset()
            NotificationCenter.default.removeObserver(self, name: .NSManagedObjectContextDidSave, object: self.targetImportContext)
        }
        self.targetImportContext = nil
    }
    
    @objc func targetImportContextDidSave(notification: Notification) {
        if let context = notification.object
        {
            (context as! NSManagedObjectContext).perform {
                NotificationCenter.default.post(name: Notification.Name("CoreDataAdapterDidImportChangesNotification"), object: context, userInfo:notification.userInfo);
            }
        }
        else
        {
            debugPrint("targetImportContextDidSave.cannot find context")
        }
    }
    
    func deleteAllPendingRelationships() {
        guard let pendingRelationships = try? privateContext.executeFetchRequest(entityName: "QSPendingRelationship") as? [QSPendingRelationship] else { return }
        pendingRelationships.forEach {
            self.privateContext.delete($0)
        }
    }
    
    func deleteInsertedButUnmergedEntities() {
        let pendingEntities = fetchEntities(state: .inserted)
        pendingEntities.forEach {
            $0.entityState = .synced
        }
    }
    
    func updateInsertedEntitiesAndSave() {
        for pending in self.fetchEntities(state: .inserted) {
            pending.entityState = .synced
        }
        savePrivateContext()
    }
    
    func nextStateToSync(after state: SyncedEntityState) -> SyncedEntityState {
        return SyncedEntityState(rawValue: state.rawValue + 1)!
    }
    
    func shouldIgnoreRelationship(key: String) -> Bool {
        if self.isShared()
        {
            if key == "folder"
            {
                return true
            }
        }
        return false
    }
    
    func shouldIgnore(key: String) -> Bool {
        if key == CoreDataAdapter.timestampKey || CloudKitSynchronizer.metadataKeys.contains(key) || key == "ckOwnerName"
        {
            return true
        }
        if self.isShared()
        {
            if key == "sortOrder" || key == "closed" || key == "eBankType" || key == "eBankURL" || key == "eBankBankID" || key == "eBankBankName" || key == "eBankAtriumMember" || key == "eBankAccountNumber" || key == "eBankStartDate" || key == "eBankAccountID" || key == "eBankAccountType" || key == "eBankAccountIsActivated" || key == "eBankSaltEdgeLoginSecret" || key == "folder"
            {
                return true
            }
        }
        return false
    }
    
    func transformedValue(_ value: Any, valueTransformerName: String?) -> Any? {
        if let valueTransformerName = valueTransformerName {
            let transformer = ValueTransformer(forName: NSValueTransformerName(valueTransformerName))
            return transformer?.transformedValue(value)
        } else {
            return QSCoder.shared.data(from: value)
        }
    }
    
    func reverseTransformedValue(_ value: Any, valueTransformerName: String?) -> Any? {
        if let valueTransformerName = valueTransformerName {
            let transformer = ValueTransformer(forName: NSValueTransformerName(valueTransformerName))
            return transformer?.reverseTransformedValue(value)
        } else if let data = value as? Data {
            return QSCoder.shared.object(from: data)
        } else {
            return nil
        }
    }
    
    func threadSafePrimaryKeyValue(for object: NSManagedObject) -> String {
        var identifier: String! = nil
        object.managedObjectContext!.performAndWait {
            identifier = self.uniqueIdentifier(for: object)
        }
        return identifier
    }
}

//MARK: - Object Identifiers
extension CoreDataAdapter {
    func identifierFieldName(forEntity entityName: String) -> String {
        return entityPrimaryKeys[entityName]!;
    }
    
    func uniqueIdentifier(for object: NSManagedObject) -> String? {
        guard let entityName = object.entity.name else { return nil }
            
        let key = identifierFieldName(forEntity: entityName)
        return object.value(forKey: key) as? String
    }
    
    func uniqueIdentifier(forObjectFrom record: CKRecord) -> String {
        let entityType = record.recordType
        let name = record.recordID.recordName
        let index = name.index(name.startIndex, offsetBy: entityType.count + 1)
        return String(name[index...])
    }
    
    public func updateTrackingForObjectsWithPrimaryKey()
    {
        if self.isShared()
        {
            return
        }
        debugPrint("updateTrackingForObjectsWithPrimaryKey()")
        self.privateContext.perform {
            let noPrimaryKeyTrackingEntities = try? self.privateContext.executeFetchRequest(entityName: "QSSyncedEntity",
                                                                       predicate: NSPredicate(format: "originObjectID beginswith[cd] %@", "x-coredata://"),
                                                                       fetchLimit: 0) as? [QSSyncedEntity]
            if noPrimaryKeyTrackingEntities!.count > 0
            {
                debugPrint("needs fix: ", noPrimaryKeyTrackingEntities!.count)
                noPrimaryKeyTrackingEntities?.forEach({ (syncedEntity) in
                    let entityName = syncedEntity.entityType
                    let oldOriginObjectID = syncedEntity.originObjectID!
                    self.targetContext.perform {
                        if let objectID = self.targetContext.persistentStoreCoordinator?.managedObjectID(forURIRepresentation:URL(string: oldOriginObjectID)!)
                        {
                            let managedObject = self.targetContext.object(with: objectID)
                            let newIdentifier = self.uniqueIdentifier(for: managedObject)
                            self.privateContext.perform {

                                let syncedEntityWithNewIdentifier = self.syncedEntity(withOriginIdentifier: newIdentifier!)
                                if syncedEntityWithNewIdentifier == nil
                                {
                                    debugPrint("will fix ", oldOriginObjectID, " -> ", newIdentifier)
                                    syncedEntity.originObjectID = newIdentifier
                                    self.savePrivateContext()
                                }
                                else
                                {
                                    debugPrint("cannot fix ", oldOriginObjectID, " -> ", newIdentifier, "(newIdentifier already being tracked)")
                                }
                            }
                        }
                        else
                        {
                            debugPrint("cannot find object with id: ", syncedEntity.originObjectID)
                        }
                    }
                    
                })
                self.savePrivateContext()
            }
        }
    }
}

//MARK: - Entities
extension CoreDataAdapter {
    func createSyncedEntity(identifier: String, entityName: String) {
        
        guard let entityDescription = NSEntityDescription.entity(forEntityName: "QSSyncedEntity", in: privateContext) else { return }
        let syncedEntity = QSSyncedEntity(entity: entityDescription, insertInto: privateContext)
        
        syncedEntity.entityType = entityName
        syncedEntity.entityState = .new
        syncedEntity.updatedDate = NSDate()
        syncedEntity.originObjectID = identifier
        syncedEntity.identifier = "\(entityName).\(identifier)"
    }
    
    func createSyncedEntity(share: CKShare) -> QSSyncedEntity? {
        let entityForShare = QSSyncedEntity(entity: NSEntityDescription.entity(forEntityName: "QSSyncedEntity", in: privateContext)!,
                                            insertInto: privateContext)
        entityForShare.entityType = "CKShare"
        entityForShare.identifier = share.recordID.recordName
        entityForShare.updatedDate = NSDate()
        entityForShare.entityState = .synced
        return entityForShare
    }
    
    func createSyncedEntity(record: CKRecord) -> QSSyncedEntity? {
        let syncedEntity = QSSyncedEntity(entity: NSEntityDescription.entity(forEntityName: "QSSyncedEntity", in: privateContext)!,
                                          insertInto: privateContext)
        syncedEntity.identifier = record.recordID.recordName
        let entityName = record.recordType
        syncedEntity.entityType = entityName
        syncedEntity.updatedDate = NSDate()
        syncedEntity.entityState = .inserted
        
        var objectID: String!
        targetImportContext.performAndWait {
            let object = self.insertManagedObject(entityName: entityName)
            objectID = self.uniqueIdentifier(forObjectFrom: record)
            object.setValue(objectID, forKey: self.identifierFieldName(forEntity: entityName))
            if self.isShared()
            {
                object.setValue(self.recordZoneID.ownerName, forKey: "ckOwnerName")
            }
        }
        
        syncedEntity.originObjectID = objectID
        return syncedEntity
    }
    
    func syncedEntity(withOriginIdentifier identifier: String) -> QSSyncedEntity? {
        guard privateContext != nil else {
            return nil
        }
        let fetched = try? self.privateContext.executeFetchRequest(entityName: "QSSyncedEntity",
                                                                   predicate: NSPredicate(format: "originObjectID == %@", identifier),
                                                                   fetchLimit: 1) as? [QSSyncedEntity]
        return fetched?.first
    }
    
    func syncedEntity(withIdentifier identifier: String) -> QSSyncedEntity? {
        guard privateContext != nil else {
            return nil
        }
        let fetched = try? self.privateContext.executeFetchRequest(entityName: "QSSyncedEntity",
                                                                   predicate: NSPredicate(format: "identifier == %@", identifier),
                                                                   fetchLimit: 1) as? [QSSyncedEntity]
        return fetched?.first
    }
    
    func fetchEntities(state: SyncedEntityState) -> [QSSyncedEntity] {
        guard privateContext != nil else {
            return []
        }
        return try! privateContext.executeFetchRequest(entityName: "QSSyncedEntity", predicate: NSPredicate(format: "state == %lud", state.rawValue)) as! [QSSyncedEntity]
    }
    
    func fetchEntities(identifiers: [String]) -> [QSSyncedEntity] {
        guard privateContext != nil else {
            return []
        }
        return try! privateContext.executeFetchRequest(entityName: "QSSyncedEntity",
                                                       predicate: NSPredicate(format: "identifier IN %@", identifiers),
                                                       preload: true) as! [QSSyncedEntity]
    }

    func fetchEntities(originObjectIDs: [String]) -> [QSSyncedEntity] {
        guard privateContext != nil else {
            return []
        }
        return try! privateContext.executeFetchRequest(entityName: "QSSyncedEntity",
                                                       predicate: NSPredicate(format: "originObjectID IN %@", originObjectIDs),
                                                       preload: true) as! [QSSyncedEntity]
    }

    func delete(syncedEntities: [QSSyncedEntity]) {
        var identifiersByType = [String: [String]]()
        for syncedEntity in syncedEntities {
            
            if let originObjectID = syncedEntity.originObjectID,
                let entityType = syncedEntity.entityType,
                entityType != "CKShare" && syncedEntity.entityState != .deleted {
                if identifiersByType[entityType] == nil {
                    identifiersByType[entityType] = [String]()
                }
                identifiersByType[entityType]?.append(originObjectID)
            }
            
            privateContext.delete(syncedEntity)
        }
        
        targetImportContext.performAndWait {
            identifiersByType.forEach({ (entityType, identifiers) in
                let objects = self.managedObjects(entityName: entityType,
                                                  identifiers: identifiers,
                                                  context: self.targetImportContext)
                objects.forEach {
                    self.targetImportContext.delete($0)
                }
            })
        }
    }
    
    func save(record: CKRecord, for entity: QSSyncedEntity) {
        var qsRecord: QSRecord! = entity.record
        if qsRecord == nil {
            qsRecord = QSRecord(entity: NSEntityDescription.entity(forEntityName: "QSRecord", in: privateContext)!,
                                insertInto: privateContext)
            entity.record = qsRecord
        }
        qsRecord.encodedRecord = QSCoder.shared.encode(record) as NSData
    }
    
    func storedRecord(for entity: QSSyncedEntity) -> CKRecord? {
        
        guard let qsRecord = entity.record,
            let data = qsRecord.encodedRecord else {
                return nil
        }
        
        return QSCoder.shared.decode(from: data as Data)
    }
    
    func storedShare(for entity: QSSyncedEntity) -> CKShare? {
        var share: CKShare?
        if let shareData = entity.share?.record?.encodedRecord {
            share = QSCoder.shared.decode(from: shareData as Data)
        }
        return share
    }
    
    func save(share: CKShare, for entity: QSSyncedEntity) {
        var qsRecord: QSRecord!
        if entity.share == nil {
            entity.share = createSyncedEntity(share: share)
            qsRecord = QSRecord(entity: NSEntityDescription.entity(forEntityName: "QSRecord", in: self.privateContext)!,
                                    insertInto: self.privateContext)
            entity.share?.record = qsRecord
        } else {
            qsRecord = entity.share?.record
        }
        qsRecord.encodedRecord = QSCoder.shared.encode(share) as NSData
    }
    
    func recordsToUpload(state: SyncedEntityState, limit: Int) throws -> [CKRecord] {
        var recordsArray = [CKRecord]()
        debugPrint("getting recordsToUpload with state:", state)
        var duplicateDetected = false
        privateContext.performAndWait {
            let entities = sortedEntities(entities:fetchEntities(state: state))
            var pending : [QSSyncedEntity] = Array(entities.reversed()) // loop takes objects from the back, therefore we need reversed array here
            var includedEntityIDs = Set<String>()
            while recordsArray.count < limit && !pending.isEmpty {
                var entity: QSSyncedEntity! = pending.last
                if (entity != nil && includedEntityIDs.contains(entity.identifier!))
                {
                    ddPrint("already included. duplicate?")
                    duplicateDetected = true
                    if let index = pending.firstIndex(of: entity) {
                        pending.remove(at: index)
                    }
                }
                var nilRecordsIdentifiers = [String]()
                while entity != nil && entity.entityState == state && !includedEntityIDs.contains(entity.identifier!) {
                    var parentEntity: QSSyncedEntity? = nil
                    if let index = pending.firstIndex(of: entity) {
                        pending.remove(at: index)
                    }
                    let record = self.recordToUpload(for: entity, context: self.targetContext, parentEntity: &parentEntity)
                    if (record != nil)
                    {
                        recordsArray.append(record!)
                        includedEntityIDs.insert(entity.identifier!)
                        entity = parentEntity
                    }
                    else
                    {
                        if (entity.identifier != nil)
                        {
                            nilRecordsIdentifiers.append(entity.identifier!)
                        }
                        entity = nil
                    }
                }
                if (nilRecordsIdentifiers.count > 0)
                {
                    debugPrint("records are nil for identifiers:", nilRecordsIdentifiers)
                }
            }
            debugPrint("recordsToUpload IDs: ", includedEntityIDs)
        }
        if (duplicateDetected)
        {
            throw CloudKitSynchronizer.SyncError.corruptedData
        }
        debugPrint("return recordsToUpload with state:", state, "count:", recordsArray.count, "ids:")
        return recordsArray
    }
    
    func recordToUpload(for entity: QSSyncedEntity, context: NSManagedObjectContext, parentEntity: inout QSSyncedEntity?) -> CKRecord? {
        var record: CKRecord! = storedRecord(for: entity)
        if record == nil {
            record = CKRecord(recordType: entity.entityType!,
                              recordID: CKRecord.ID(recordName: entity.identifier!, zoneID: recordZoneID))
        }
        
        var originalObject: NSManagedObject!
        var entityDescription: NSEntityDescription!
        let objectID = entity.originObjectID!
        let entityState = entity.entityState
        let entityType = entity.entityType!
        let changedKeys = entity.changedKeysArray
        
        context.performAndWait {
            originalObject = self.managedObject(entityName: entityType, identifier: objectID, context: context)
            if (originalObject == nil)
            {
                // originalObject not found. Probably shared ckOwner mismatch (shared synchronizer and not shared object or different owners)
                return
            }
            entityDescription = NSEntityDescription.entity(forEntityName: entityType, in: context)
            let primaryKey = self.identifierFieldName(forEntity: entityType)
            // Add attributes
            entityDescription.attributesByName.forEach({ (attributeName, attributeDescription) in
                if attributeName != primaryKey && !self.shouldIgnore(key: attributeName) &&
                    (entityState == .new || changedKeys.contains(attributeName)) {
                    let value = originalObject.value(forKey: attributeName)
                    if attributeDescription.attributeType == .binaryDataAttributeType && !self.forceDataTypeInsteadOfAsset,
                        let data = value as? Data {
                        let fileURL = self.tempFileManager.store(data: data)
                        let asset = CKAsset(fileURL: fileURL)
                        debugPrint("ckasset fileURL :", fileURL)
                        record[attributeName] = asset
                    } else if attributeDescription.attributeType == .transformableAttributeType,
                        let value = value,
                        let transformed = self.transformedValue(value, valueTransformerName: attributeDescription.valueTransformerName) as? CKRecordValueProtocol{
                        record[attributeName] = transformed
                    } else {
                        record[attributeName] = value as? CKRecordValueProtocol
                    }
                }
            })
            
            // to trigger CKRecord's setValue: forKey: for nil x-to-one relationships. Otherwise this field for CKRecord is not being updates
            entityDescription.relationshipsByName.forEach({ (relationshipName, relationshipDescription) in
                if ((entityState == .new || changedKeys.contains(relationshipName)) && !self.shouldIgnoreRelationship(key: relationshipName))
                {
                    let value = originalObject.value(forKey: relationshipName)
                    if (value == nil && !relationshipDescription.isToMany)
                    {
                        record[relationshipName] = nil
                    }
                }
            })
        }
        
        if (originalObject == nil)
        {
            return nil;
        }
        
        let entityClass: AnyClass? = NSClassFromString(entityDescription.managedObjectClassName)
        var parentKey: String?
        if let parentKeyClass = entityClass as? ParentKey.Type {
            parentKey = parentKeyClass.parentKey()
        }
        
        let referencedEntities = referencedSyncedEntitiesByReferenceName(for: originalObject, context: context)

        referencedEntities.forEach { (relationshipName, entityOrArray) in
            if (entityState == .new || changedKeys.contains(relationshipName)) && !self.shouldIgnoreRelationship(key: relationshipName) {
                if (entityOrArray is NSArray)
                {
                    let entityArray = entityOrArray as! NSArray
                    let references = NSMutableArray()
                    entityArray.forEach {
                        let entity = $0 as! QSSyncedEntity
                        let recordID = CKRecord.ID(recordName: entity.identifier!, zoneID: self.recordZoneID)
                        let recordReference = CKRecord.Reference(recordID: recordID, action: .none)
                        references.add(recordReference)
                    }
                    if references.count > 0
                    {
                        record[relationshipName] = references
                    }
                    else
                    {
                        record[relationshipName] = nil
                    }
                }
                else
                {
                    let entity = entityOrArray as! QSSyncedEntity
                    let recordID = CKRecord.ID(recordName: entity.identifier!, zoneID: self.recordZoneID)
                    // if we set the parent we must make the action .deleteSelf, otherwise we get errors if we ever try to delete the parent record
                    // with deleteSelf sharable children count is 750, with .none - much bigger (?). We just need to handle correct deletions upload order to avoid reference violation errors.
                    let action: CKRecord.Reference.Action = parentKey == relationshipName ? .none : .none
                    let recordReference = CKRecord.Reference(recordID: recordID, action: action)
                    record[relationshipName] = recordReference
                }
            }
        }
        
        if let parentKey = parentKey,
            let reference = record[parentKey] as? CKRecord.Reference,
            (entityState == .new || changedKeys.contains(parentKey) || record.parent?.recordID != reference.recordID)
             {
            // For the parent reference we have to use action .none though, even if we must use .deleteSelf for the attribute (see ^)
            debugPrint("update parent for: ", record.recordID, "entityState: ", entityState, "changedKeys: ", changedKeys)
            record.parent = CKRecord.Reference(recordID: reference.recordID, action: CKRecord.Reference.Action.none)
            parentEntity = referencedEntities[parentKey] as? QSSyncedEntity
        }
        else if self.shouldShareEntity(entity: entity)
        {
            let extraDataParentRecordID = self.extraDataParentRecordID()
            if extraDataParentRecordID != nil
            {
                // For the parent reference we have to use action .none though, even if we must use .deleteSelf for the attribute (see ^)
                record.parent = CKRecord.Reference(recordID: extraDataParentRecordID!, action: CKRecord.Reference.Action.none)
            }
        }
        else if (entity.entityType == self.extraDataEntityName())
        {
            self.updateSharableEntitiesParentKeys(entity, record)
        }
        
        record[CoreDataAdapter.timestampKey] = entity.updatedDate
        
        return record
    }
    
    func referencedSyncedEntitiesByReferenceName(for object: NSManagedObject, context: NSManagedObjectContext) -> [String: Any] {
        var objectIDsByRelationshipName: [String: Any]!
        context.performAndWait {
            objectIDsByRelationshipName = self.referencedObjectIdentifiersByRelationshipName(for: object)
        }
        
        var entitiesByName = [String: Any]()
        objectIDsByRelationshipName.forEach { (relationshipName, identifierOrSet) in
            if identifierOrSet is NSArray {
                let identifiersSet = identifierOrSet as! NSArray
                let entities = NSMutableArray()
                identifiersSet.forEach {
                    if let entity = self.syncedEntity(withOriginIdentifier: $0 as! String) {
                        entities.add(entity)
                    }
                    else
                    {
                        debugPrint("BAD.toMany. syncedEntity not found with identifier", $0)
                    }
                }
                entitiesByName[relationshipName] = entities
            }
            else
            {
                let identifier = identifierOrSet as! String
                if let entity = self.syncedEntity(withOriginIdentifier: identifier) {
                    entitiesByName[relationshipName] = entity
                }
                else
                {
                    debugPrint("BAD.toOne. syncedEntity not found with identifier", identifier)
                }
            }
        }
        return entitiesByName
    }
    
    func referencedObjectIdentifiersByRelationshipName(for object: NSManagedObject) -> [String: Any] {
        var objectIDs = [String: Any]()
        object.entity.relationshipsByName.forEach { (name, relationshipDescription) in
            if !relationshipDescription.isToMany,
                let referencedObject = object.value(forKey: name) as? NSManagedObject {
                objectIDs[relationshipDescription.name] = self.uniqueIdentifier(for: referencedObject)
            }
            else if relationshipDescription.inverseRelationship!.isToMany,
                            let referencedObjects = object.value(forKey: name) as? NSSet {
                // many-to-many NSSet
                let identifiers = NSMutableArray()
                referencedObjects.forEach {
                    if let identifier = self.uniqueIdentifier(for: $0 as! NSManagedObject)
                    {
                        identifiers.add(identifier)
                    }
                    else
                    {
                        debugPrint("identifier is nil for", $0)
                    }
                }
                objectIDs[relationshipDescription.name] = identifiers
            }
            else if relationshipDescription.inverseRelationship!.isToMany,
                            let referencedObjects = object.value(forKey: name) as? NSOrderedSet {
                // many-to-many NSOrderedSet
                let identifiers = NSMutableArray()
                referencedObjects.forEach {
                    if let identifier = self.uniqueIdentifier(for: $0 as! NSManagedObject)
                    {
                        identifiers.add(identifier)
                    }
                    else
                    {
                        debugPrint("identifier is nil for", $0)
                    }
                }
                objectIDs[relationshipDescription.name] = identifiers
            }
        }
        return objectIDs
    }
}

// MARK: - Target context
extension CoreDataAdapter {
    func insertManagedObject(entityName: String) -> NSManagedObject {
        let managedObject = NSEntityDescription.insertNewObject(forEntityName: entityName,
                                                                into: targetImportContext)
        try! targetImportContext.obtainPermanentIDs(for: [managedObject])
        if self.isShared()
        {
            managedObject.setValue(self.recordZoneID.ownerName, forKey: "ckOwnerName")
        }
        return managedObject
    }
    
    func managedObjects(entityName: String, identifiers: [String], context: NSManagedObjectContext) -> [NSManagedObject] {
        let identifierKey = identifierFieldName(forEntity: entityName)
        return try! context.executeFetchRequest(entityName: entityName,
                                                predicate: NSPredicate(format: "%K IN %@ and %K = %@", identifierKey, identifiers, "ckOwnerName", self.sharedZoneOwnerName() ?? 0)) as! [NSManagedObject]
    }
    
    func managedObject(entityName: String, identifier: String, context: NSManagedObjectContext) -> NSManagedObject? {
        let identifierKey = identifierFieldName(forEntity: entityName)
        return try? context.executeFetchRequest(entityName: entityName,
                                                predicate: NSPredicate(format: "%K == %@ and %K == %@", identifierKey, identifier, "ckOwnerName", self.sharedZoneOwnerName() ?? 0)).first as? NSManagedObject
    }
    
    func applyAttributeChanges(record: CKRecord, to object: NSManagedObject, state: SyncedEntityState, changedKeys: [String]) {
        let primaryKey = identifierFieldName(forEntity: object.entity.name!)
        if state == .changed || state == .new {
            switch mergePolicy {
            case .server:
                object.entity.attributesByName.forEach { (attributeName, attributeDescription) in
                    if !shouldIgnore(key: attributeName) && !(record[attributeName] is CKRecord.Reference) && primaryKey != attributeName {
                        assignAttributeValue(record[attributeName],
                                             toManagedObject: object,
                                             attributeName: attributeName,
                                             attributeDescription: attributeDescription)
                    }
                }
            case .client:
                object.entity.attributesByName.forEach { (attributeName, attributeDescription) in
                    if !shouldIgnore(key: attributeName) && !(record[attributeName] is CKRecord.Reference) && primaryKey != attributeName && !changedKeys.contains(attributeName) && state != .new {
                        assignAttributeValue(record[attributeName],
                                             toManagedObject: object,
                                             attributeName: attributeName,
                                             attributeDescription: attributeDescription)
                    }
                }
            case .custom:
                if let conflictDelegate = conflictDelegate {
                    var recordChanges = [String: Any]()
                    object.entity.attributesByName.forEach { (attributeName, attributeDescription) in
                        if !(record[attributeName] is CKRecord.Reference) && primaryKey != attributeName {
                            if let asset = record[attributeName] as? CKAsset {
                                if let url = asset.fileURL,
                                    let data = try? Data(contentsOf: url) {
                                    recordChanges[attributeName] = data
                                }
                            } else if let value = record[attributeName] {
                                recordChanges[attributeName] = value
                            } else {
                                recordChanges[attributeName] = NSNull()
                            }
                        }
                    }
                    conflictDelegate.coreDataAdapter(self, gotChanges: recordChanges, for: object)
                }
            }
        } else {
            object.entity.attributesByName.forEach { (attributeName, attributeDescription) in
                if !shouldIgnore(key: attributeName) && !(record[attributeName] is CKRecord.Reference) && primaryKey != attributeName {
                    assignAttributeValue(record[attributeName],
                                         toManagedObject: object,
                                         attributeName: attributeName,
                                         attributeDescription: attributeDescription)
                }
            }
        }
    }
    
    func assignAttributeValue(_ value: Any?, toManagedObject object: NSManagedObject, attributeName: String, attributeDescription: NSAttributeDescription) {
        if let value = value as? CKAsset {
            guard let url = value.fileURL,
            let data = try? Data(contentsOf: url) else { return }
            object.setValue(data, forKey: attributeName)
        } else if let value = value,
            attributeDescription.attributeType == .transformableAttributeType {
            object.setValue(reverseTransformedValue(value,
                                                    valueTransformerName: attributeDescription.valueTransformerName),
                            forKey: attributeName)
        } else {
            object.setValue(value, forKey: attributeName)
        }
    }
    
    func mergeChangesIntoTargetContext(completion: @escaping (Error?)->()) {
        debugPrint("Requesting save")
        delegate.coreDataAdapter(self, requestsContextSaveWithCompletion: { (error) in
            guard error == nil else {
                completion(error)
                return
            }
            
//            self.isMergingImportedChanges = true
            debugPrint("Now importing")
            self.delegate.coreDataAdapter(self, didImportChanges: self.targetImportContext, completion: { (error) in
//                self.isMergingImportedChanges = false
                debugPrint("Saved imported changes")
                completion(error)
            })
        })
    }
    
    func childrenRecords(for entity: QSSyncedEntity) -> [CKRecord] {
        // Add record for this entity
        var childrenRecords = [CKRecord]()
        var parent: QSSyncedEntity?
        let recordToUpload = self.recordToUpload(for: entity, context: targetContext, parentEntity: &parent)
        if (recordToUpload != nil)
        {
            childrenRecords.append(recordToUpload!)
        }
        
        let relationships = childrenRelationships[entity.entityType!] ?? []
        for relationship in relationships {
            // get child objects using parentkey
            let objectID = entity.originObjectID!
            let entityType = entity.entityType!
            var originalObject: NSManagedObject!
            var childrenIdentifiers = [String]()
            targetContext.performAndWait {
                originalObject = self.managedObject(entityName: entityType, identifier: objectID, context: self.targetContext)
                let childrenObjects = self.children(of: originalObject, relationship: relationship)
                childrenIdentifiers.append(contentsOf: childrenObjects.compactMap { self.uniqueIdentifier(for: $0) })
            }
            // get their syncedEntities
            for identifier in childrenIdentifiers {
                if let childEntity = self.syncedEntity(withOriginIdentifier: identifier) {
                    // add and also add their children
                    childrenRecords.append(contentsOf: self.childrenRecords(for: childEntity))
                }
            }
        }
        
        return childrenRecords
    }
    
    func children(of parent: NSManagedObject, relationship: ChildRelationship) -> [NSManagedObject] {
        let predicate = NSPredicate(format: "%K == %@", relationship.childParentKey, parent)
        return (try? parent.managedObjectContext?.executeFetchRequest(entityName: relationship.childEntityName, predicate: predicate) as? [NSManagedObject]) ?? []
    }
}

// MARK: - Pending relationships
extension CoreDataAdapter {
    func prepareRelationshipChanges(for object: NSManagedObject, record: CKRecord) -> [String] {
        var relationships = [String]()
        for relationshipName in object.entity.relationshipsByName.keys {
            if object.entity.relationshipsByName[relationshipName]!.isToMany && (object.entity.relationshipsByName[relationshipName]!.inverseRelationship != nil) &&
                !object.entity.relationshipsByName[relationshipName]!.inverseRelationship!.isToMany {
                continue
            }
            
            if record[relationshipName] != nil {
                relationships.append(relationshipName)
            } else {
                object.setValue(nil, forKey: relationshipName)
            }
        }
        return relationships
    }
    
    func saveRelationshipChanges(record: CKRecord, names: [String], entity: QSSyncedEntity) {
        for key in names {
            if !self.shouldIgnoreRelationship(key: key)
            {
                if let reference = record[key] as? CKRecord.Reference {
                    let relationship = QSPendingRelationship(entity: NSEntityDescription.entity(forEntityName: "QSPendingRelationship", in: privateContext)!,
                                                             insertInto: privateContext)
                    relationship.relationshipName = key
                    relationship.targetIdentifier = reference.recordID.recordName
                    relationship.forEntity = entity
                }
                else if let reference = record[key] as? [CKRecord.Reference] {
                    let targetIdentifiers = NSMutableArray()
                    reference.forEach {
                        targetIdentifiers.add($0.recordID.recordName)
                    }
                    let relationship = QSPendingRelationship(entity: NSEntityDescription.entity(forEntityName: "QSPendingRelationship", in: self.privateContext)!,
                                                             insertInto: self.privateContext)
                    relationship.relationshipName = key
                    relationship.targetIdentifier = targetIdentifiers.componentsJoined(by:",")
                    relationship.forEntity = entity
                }
            }
        }
    }
    
    func saveShareRelationship(for entity: QSSyncedEntity, record: CKRecord) {
        if let share = record.share {
            let relationship = QSPendingRelationship(entity: NSEntityDescription.entity(forEntityName: "QSPendingRelationship", in: privateContext)!,
                                                     insertInto: privateContext)
            relationship.relationshipName = "share"
            relationship.targetIdentifier = share.recordID.recordName
            relationship.forEntity = entity
        }
    }
    
    func entitiesWithPendingRelationships() -> [QSSyncedEntity] {
        let fetchRequest = NSFetchRequest<NSFetchRequestResult>()
        fetchRequest.entity = NSEntityDescription.entity(forEntityName: "QSSyncedEntity", in: privateContext)!
        fetchRequest.resultType = .managedObjectResultType
        fetchRequest.predicate = NSPredicate(format: "pendingRelationships.@count != 0")
        fetchRequest.returnsObjectsAsFaults = false
        fetchRequest.relationshipKeyPathsForPrefetching = ["originIdentifier", "pendingRelationships"]
        return try! privateContext.fetch(fetchRequest) as! [QSSyncedEntity]
    }

    func allEntities() -> [QSSyncedEntity] {
        let fetchRequest = NSFetchRequest<NSFetchRequestResult>()
        fetchRequest.entity = NSEntityDescription.entity(forEntityName: "QSSyncedEntity", in: privateContext)!
        fetchRequest.resultType = .managedObjectResultType
        fetchRequest.returnsObjectsAsFaults = false
        fetchRequest.relationshipKeyPathsForPrefetching = ["originIdentifier", "pendingRelationships"]
        return try! privateContext.fetch(fetchRequest) as! [QSSyncedEntity]
    }

    func pendingShareRelationship(for entity: QSSyncedEntity) -> QSPendingRelationship? {
        
        return (entity.pendingRelationships as? Set<QSPendingRelationship>)?.first {
            $0.relationshipName ?? "" == "share"
        }
    }
    
    func originObjectIdentifier(forEntityWithIdentifier identifier: String) -> RelationshipTarget? {
        guard let result = try? privateContext.executeFetchRequest(entityName: "QSSyncedEntity",
                                                       predicate: NSPredicate(format: "identifier == %@", identifier),
                                                       fetchLimit: 1,
                                                       resultType: .dictionaryResultType,
                                                       propertiesToFetch: ["originObjectID", "entityType"]).first,
            let dictionary = result as? [String: String] else {
                return nil
        }
        
        return RelationshipTarget(originObjectID: dictionary["originObjectID"], entityType: dictionary["entityType"])
    }
    
    func pendingRelationshipTargetIdentifiers(for entity: QSSyncedEntity) -> [String: Any] {
        guard let pending = entity.pendingRelationships as? Set<QSPendingRelationship> else { return [:] }
        var relationships = [String: Any]()
        
        for pendingRelationship in pending {
            if pendingRelationship.relationshipName == "share" {
                continue
            }
            
            let targetIdentierString = pendingRelationship.targetIdentifier!
            let identifiers = targetIdentierString.components(separatedBy: ",") as [String]
            
            if identifiers.count <= 1
            {
                if let targetObjectInfo = originObjectIdentifier(forEntityWithIdentifier: targetIdentierString) {
                    relationships[pendingRelationship.relationshipName!] = targetObjectInfo
                }
                else
                {
                    debugPrint("originObjectIdentifier not found for entity with identifier: ", targetIdentierString)
                }
            }
            else
            {
                let targetObjectInfos = NSMutableArray()
                identifiers.forEach {
                    if let targetObjectInfo = originObjectIdentifier(forEntityWithIdentifier: $0) {
                        targetObjectInfos.add(targetObjectInfo)
                    }
                    else
                    {
                        debugPrint("originObjectIdentifier not found for entity with identifier: ", $0)
                    }
                }
                relationships[pendingRelationship.relationshipName!] = targetObjectInfos
            }
            
        }
        return relationships
    }
    
    func applyPendingRelationships() {
        
        //Need to save before we can use NSDictionaryResultType, which greatly speeds up this step
        self.savePrivateContext()
        
        let entities = entitiesWithPendingRelationships()
        var queriesByEntityType = [String: [String: QueryData]]()
        for entity in entities {
            
            guard entity.entityState != .deleted else { continue }
            
            var pendingCount = entity.pendingRelationships?.count ?? 0
            if let pendingShare = pendingShareRelationship(for: entity) {
                let share = syncedEntity(withIdentifier: pendingShare.targetIdentifier!)
                entity.share = share
                pendingCount = pendingCount - 1
            }
            
            // If there was something to connect, other than the share
            if pendingCount > 0 {
                let query = QueryData(identifier: entity.originObjectID!,
                                      record: nil,
                                      entityType: entity.entityType!,
                                      changedKeys: entity.changedKeysArray,
                                      state: entity.entityState,
                                      targetRelationshipsDictionary: pendingRelationshipTargetIdentifiers(for: entity))
                if queriesByEntityType[entity.entityType!] == nil {
                    queriesByEntityType[entity.entityType!] = [String: QueryData]()
                }
                queriesByEntityType[entity.entityType!]![entity.originObjectID!] = query
            }
            
            (entity.pendingRelationships as? Set<QSPendingRelationship>)?.forEach {
                self.privateContext.delete($0)
            }
        }
        
        // Might not need to dispatch if there's nothing to connect
        guard queriesByEntityType.count > 0 else { return }
        
        targetImportContext.performAndWait {
            self.targetApply(pendingRelationships: queriesByEntityType, context: self.targetImportContext)
        }
    }
    
    func targetApply(pendingRelationships: [String: [String: QueryData]], context: NSManagedObjectContext) {
        debugPrint("Target apply pending relationships")
        
        pendingRelationships.forEach { (entityType, queries) in
            
            let objects = self.managedObjects(entityName: entityType,
                                              identifiers: Array(queries.keys),
                                              context: context)
            for managedObject in objects {
                guard let identifier = self.uniqueIdentifier(for: managedObject),
                    let query = queries[identifier] else { continue }
                query.targetRelationshipsDictionary?.forEach({ (relationshipName, targetOrTargetsArray) in
                    let shouldApplyTarget = query.state.rawValue > SyncedEntityState.changed.rawValue ||
                        self.mergePolicy == .server ||
                        (self.mergePolicy == .client && (!query.changedKeys.contains(relationshipName) || (query.state == .new && managedObject.value(forKey: relationshipName) == nil)))
                    
                    let isToMany = managedObject.entity.relationshipsByName[relationshipName]!.isToMany
                    var targetOrArray = targetOrTargetsArray
                    if isToMany && targetOrArray is RelationshipTarget {
                        targetOrArray = [ targetOrArray ]
                    }
                    if targetOrArray is RelationshipTarget {
                        let target = targetOrArray as! RelationshipTarget
                        if let entityType = target.entityType,
                            let originObjectID = target.originObjectID,
                            shouldApplyTarget {
                            let targetManagedObject = self.managedObject(entityName: entityType,
                                                                         identifier: originObjectID,
                                                                         context: context)
                            if targetManagedObject == nil {
                                debugPrint("relationship object not found for key: ", relationshipName, ", with identifier: ", originObjectID, " for managedObject entityName : ", managedObject.entity.name ?? "n/a")
                            }
                            managedObject.setValue(targetManagedObject, forKey: relationshipName)
                        } else if self.mergePolicy == .custom,
                            let entityType = target.entityType,
                            let originObjectID = target.originObjectID,
                            let conflictDelegate = self.conflictDelegate,
                            let targetManagedObject = self.managedObject(entityName: entityType,
                                                                         identifier: originObjectID,
                                                                         context: context) {
                            
                            conflictDelegate.coreDataAdapter(self,
                                                             gotChanges: [relationshipName: targetManagedObject],
                                                             for: managedObject)
                        }
                        else
                        {
                            debugPrint(".custom relationship object not found for key: ", relationshipName, ", with identifier: ", target.originObjectID ?? "n/a", " for managedObject entityName : ", managedObject.entity.name ?? "n/a")
                        }
                    }
                    else if targetOrArray is NSArray
                    {
                        let targetObjectInfos = targetOrArray as! NSArray
                        let isOrdered = managedObject.entity.relationshipsByName[relationshipName]!.isOrdered
                        let targetObjectsOrdered = NSMutableOrderedSet()
                        targetObjectInfos.forEach {
                            let originObjectID = ($0 as! RelationshipTarget).originObjectID
                            let entityType = ($0 as! RelationshipTarget).entityType
                            let targetManagedObject = self.managedObject(entityName: entityType as! String,
                                                                         identifier: originObjectID as! String,
                                                                         context: context)
                            if targetManagedObject != nil
                            {
                                targetObjectsOrdered.add(targetManagedObject!)
                            }
                            else
                            {
                                debugPrint("relationship object not found for key: ", relationshipName, "with identifier: ", originObjectID , "for managedObject entityName : ", managedObject.entity.name ?? "n/a")
                            }
                        }
                        if (isOrdered)
                        {
                            if shouldApplyTarget {
                                managedObject.setValue(targetObjectsOrdered, forKey: relationshipName)
                            }
                            else if self.mergePolicy == .custom, let conflictDelegate = self.conflictDelegate
                            {
                                conflictDelegate.coreDataAdapter(self,
                                                                 gotChanges: [relationshipName: targetObjectsOrdered],
                                                                 for: managedObject)
                            }
                        }
                        else
                        {
                            let targetObjects = targetObjectsOrdered.set
                            if shouldApplyTarget {
                                managedObject.setValue(targetObjects, forKey: relationshipName)
                            }
                            else if self.mergePolicy == .custom, let conflictDelegate = self.conflictDelegate
                            {
                                conflictDelegate.coreDataAdapter(self,
                                                                 gotChanges: [relationshipName: targetObjects],
                                                                 for: managedObject)
                            }
                        }
                    }
                })
            }
        }
    }
    
    func shouldShareEntity(entity:QSSyncedEntity) -> Bool
    {
        if (self.sharableEntities().contains(entity.entityType!))
        {
            return true
        }
        return false
    }
    
    func sharableEntities() -> [String]
    {
        return ["Payee", "Category","TradableAsset", "TradableAssetInfo", "Tag", "Currency", "Attachment", "Icon", "PayeePlacemark" ]
    }
    
    func extraDataEntityName() -> String
    {
        return "SharedData"
    }
    
    func extraDataParentRecordID() -> CKRecord.ID?
    {
        let predicate = NSPredicate(format: "entityType == %@", self.extraDataEntityName())
        let fetchedObjects = try? self.privateContext?.executeFetchRequest(entityName:"QSSyncedEntity", predicate: predicate) as? [QSSyncedEntity]
        if (fetchedObjects!.count > 0)
        {
            let syncedEntity = (fetchedObjects?.first)!
            let recordID = recordIDForSyncedEntity(syncedEntity)
            return recordID
        }
        return nil
    }
    
    func updateSharableEntitiesParentKeys(_ entity:QSSyncedEntity, _ sharedDataRecord:CKRecord)
    {
        entity.managedObjectContext!.perform
        {
            guard let entityDescription = NSEntityDescription.entity(forEntityName: "QSSyncedEntity", in: entity.managedObjectContext!) else { return }
            let fetchRequest = NSFetchRequest<NSFetchRequestResult>()
            fetchRequest.entity = entityDescription
            fetchRequest.resultType = .managedObjectResultType
            let predicate = NSPredicate(format: "entityType in %@", self.sharableEntities())
            fetchRequest.predicate = predicate
            fetchRequest.returnsObjectsAsFaults = false
            let entitiesToUpdate = try? entity.managedObjectContext?.fetch(fetchRequest) as? [QSSyncedEntity]
            entitiesToUpdate!.forEach {
                let record = self.storedRecord(for: $0 as QSSyncedEntity)
                if (record != nil)
                {
                    let parentReference = record!.parent
                    if (parentReference == nil || parentReference!.recordID != sharedDataRecord.recordID)
                    {
                        record!.parent = CKRecord.Reference(recordID: sharedDataRecord.recordID, action: .none)
                        if ($0.entityState == .synced)
                        {
                            debugPrint("update shareble. mark as changed:", entity.identifier ?? "n/a")
                            $0.entityState = .changed
                        }
                    }
                }
                else
                {
                    debugPrint("no record for syncedEntity: %@", $0)
                }
            }
        }
    }
    
    func recordIDForSyncedEntity(_ syncedEntity:QSSyncedEntity) -> CKRecord.ID
    {
        return CKRecord.ID(recordName: syncedEntity.identifier!, zoneID: self.recordZoneID)
    }
    
    func sortedEntities(entities: [QSSyncedEntity]) -> [QSSyncedEntity]
    {
        let entityNamesSorted = self.entityNamesSorted()
        let sortedEntitiesMutable = NSMutableArray(array:entities)
        entityNamesSorted.forEach {
            let predicate = NSPredicate(format: "entityType = %@", $0)
            let filteredEntities = entities.filter { predicate.evaluate(with: $0) }
            sortedEntitiesMutable.removeObjects(in:filteredEntities)
            sortedEntitiesMutable.addObjects(from:filteredEntities)
        }
        return sortedEntitiesMutable as! [QSSyncedEntity]
    }
    
    func entityNamesSorted() -> [String]
    {
        if self.targetImportContext == nil
        {
            debugPrint("entityNamesSorted. targetImportContext is nil")
            return []
        }
        let entitiesByName = self.targetImportContext.persistentStoreCoordinator?.managedObjectModel.entitiesByName
        var entityNamesSorted = [String]()
        for (entityName) in Array<String>(entitiesByName!.keys).sorted() {
            if (!entityNamesSorted.contains(entityName))
            {
                entityNamesSorted.append(entityName)
            }
            let entityDescription = entitiesByName![entityName]
            for (relationshipName) in Array<String>( entityDescription!.relationshipsByName.keys).sorted() {
                let relationshipDescription = entityDescription!.relationshipsByName[relationshipName]

                let destinationEntityName = relationshipDescription!.destinationEntity!.name
                if (destinationEntityName != entityName)
                {
                    if isOneToManyRelationship(relationshipDescription!)
                    {
                        let destinationEntityIndex = entityNamesSorted.firstIndex(of: destinationEntityName!)
                        var sourceEntityIndex = entityNamesSorted.firstIndex(of: entityName)
                        if destinationEntityIndex == NSNotFound || destinationEntityIndex == nil || destinationEntityIndex! < sourceEntityIndex!
                        {
                            // move destination entityName to the right of source (destination should be imported after source)
                            if destinationEntityIndex != NSNotFound && destinationEntityIndex != nil
                            {
                                entityNamesSorted.remove(at: destinationEntityIndex!)
                            }
                            sourceEntityIndex = entityNamesSorted.firstIndex(of: entityName)
                            entityNamesSorted.insert(destinationEntityName!, at:sourceEntityIndex!+1)
                        }
                    }
                    else if isManyToOneRelationship(relationshipDescription!)
                    {
                        let destinationEntityIndex = entityNamesSorted.firstIndex(of: destinationEntityName!)
                        var sourceEntityIndex = entityNamesSorted.firstIndex(of: entityName)
                        if destinationEntityIndex == NSNotFound || destinationEntityIndex == nil || destinationEntityIndex! > sourceEntityIndex!
                        {
                            // move destination entityName to the left of source (destination should be imported before source)
                            if destinationEntityIndex != NSNotFound && destinationEntityIndex != nil
                            {
                                entityNamesSorted.remove(at: destinationEntityIndex!)
                            }
                            sourceEntityIndex = entityNamesSorted.firstIndex(of: entityName)
                            entityNamesSorted.insert(destinationEntityName!, at:sourceEntityIndex!)
                        }
                    }
                }
            }
        }
        
        if let index = entityNamesSorted.firstIndex(of: self.extraDataEntityName()) {
            entityNamesSorted.remove(at: index)
            entityNamesSorted.insert(self.extraDataEntityName(), at:0)
        }
        return entityNamesSorted
    }
    
    func isOneToManyRelationship(_ relationshipDescription : NSRelationshipDescription) -> Bool
    {
        if relationshipDescription.isToMany && relationshipDescription.inverseRelationship != nil && !relationshipDescription.inverseRelationship!.isToMany
        {
            return true
        }
        return false
    }

    func isManyToOneRelationship(_ relationshipDescription : NSRelationshipDescription) -> Bool
    {
        if !relationshipDescription.isToMany && relationshipDescription.inverseRelationship != nil && relationshipDescription.inverseRelationship!.isToMany
        {
            return true
        }
        return false
    }
}
