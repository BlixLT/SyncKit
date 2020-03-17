//
//  CoreDataAdapter+Notifications.swift
//  SyncKit
//
//  Created by Manuel Entrena on 06/06/2019.
//  Copyright © 2019 Manuel Entrena. All rights reserved.
//

import Foundation
import CoreData

extension CoreDataAdapter {
    @objc func targetContextWillSave(notification: Notification) {
        if isMergingImportedChanges
        {
            debugPrint("targetContextWillSave.ignore. updated:", targetContext.updatedObjects)
        }
        if let object = notification.object as? NSManagedObjectContext,
            object == targetContext && !isMergingImportedChanges {
            let updated = Array(targetContext.updatedObjects)
            var identifiersAndChanges = [String: [String]]()
            for object in updated {
                var changedValueKeys = [String]()
                for key in object.changedValues().keys {
                    let relationship = object.entity.relationshipsByName[key]
                    
                    if object.entity.attributesByName[key] != nil ||
                        (relationship != nil && relationship!.isToMany == false) {
                        changedValueKeys.append(key)
                    }
                    else if relationship != nil && relationship!.isToMany && (relationship!.inverseRelationship != nil) && relationship!.inverseRelationship!.isToMany
                    {
                        changedValueKeys.append(key)
                    }
                }
                if let identifier = uniqueIdentifier(for: object),
                    changedValueKeys.count > 0 {
                    identifiersAndChanges[identifier] = changedValueKeys
                }
            }
            
            let deletedIDs: [String] = targetContext.deletedObjects.compactMap {
                if self.uniqueIdentifier(for: $0) == nil,
                    let entityName = $0.entity.name {
                    // Properties become nil when objects are deleted as a result of using an undo manager
                    // Here we can retrieve their last known identifier and mark the corresponding synced
                    // entity for deletion
                    let identifierFieldName = self.identifierFieldName(forEntity: entityName)
                    let committedValues = $0.committedValues(forKeys: [identifierFieldName])
                    return committedValues[identifierFieldName] as? String
                } else {
                    return uniqueIdentifier(for: $0)
                }
            }
            
            privateContext.perform {
                for (identifier, objectChangedKeys) in identifiersAndChanges {
                    guard let entity = self.syncedEntity(withOriginIdentifier: identifier) else { continue }
                    
                    var changedKeys = Set<String>(entity.changedKeysArray)
                    for key in objectChangedKeys {
                        changedKeys.insert(key)
                    }
                    entity.changedKeysArray = Array(changedKeys)
                    entity.entityState = .changed
                    entity.updatedDate = NSDate()
                }
                
                deletedIDs.forEach { (identifier) in
                    guard let entity = self.syncedEntity(withOriginIdentifier: identifier) else { return }
                    entity.entityState = .deleted
                    entity.updatedDate = NSDate()
                }
                
                debugPrint("QSCloudKitSynchronizer >> Will Save >> Tracking %ld updates", updated.count)
                debugPrint("QSCloudKitSynchronizer >> Will Save >> Tracking %ld deletions", deletedIDs.count)
                
                self.savePrivateContext()
            }
        }
    }
    
    @objc func targetContextDidSave(notification: Notification) {
        if isMergingImportedChanges
        {
            debugPrint("targetContextDidSave.ignore",  notification.userInfo?[NSInsertedObjectsKey] as? Set<NSManagedObject> ?? "no inserted", notification.userInfo?[NSUpdatedObjectsKey] as? Set<NSManagedObject> ?? "no updated", notification.userInfo?[NSDeletedObjectsKey] as? Set<NSManagedObject> ?? "no deleted")
        }
            if let object = notification.object as? NSManagedObjectContext,
                object == targetContext && !isMergingImportedChanges {
            var inserted = notification.userInfo?[NSInsertedObjectsKey] as? Set<NSManagedObject>
            var updated = notification.userInfo?[NSUpdatedObjectsKey] as? Set<NSManagedObject>
            let deleted = notification.userInfo?[NSDeletedObjectsKey] as? Set<NSManagedObject>
            
            let insertedMutable = NSMutableSet()
            inserted?.forEach {
                if self.areSharingIdentifiersEqual(self.sharingIdentifier(for: $0), self.sharedZoneOwnerName())
                {
                    insertedMutable.add($0)
                }
            }
            let allUpdateObjectIDs = updated?.compactMap { self.uniqueIdentifier(for: $0) } ?? []
            
            let deletedCount = deleted?.count ?? 0

            privateContext.perform {
                // get trackedObjectIDs
                let trackedObjects = self.fetchEntities(originObjectIDs:allUpdateObjectIDs)
                let trackedObjectIDs = trackedObjects.compactMap { $0.originObjectID }
                
                self.targetContext.perform {
                    
                    let updatedMutable = NSMutableSet()
                    updated?.forEach {
                        if self.areSharingIdentifiersEqual(self.sharingIdentifier(for: $0), self.sharedZoneOwnerName())
                        {
                            if let identifier = self.uniqueIdentifier(for: $0), trackedObjectIDs.contains(identifier)
                            {
                                updatedMutable.add($0)
                            }
                            else
                            {
                                insertedMutable.add($0)
                            }
                        }
                        else
                        {
                            // doesnt work if there are objects from different owners with the same uniqueID, will mark them as deleted in objectsDidChange method (when owner changes)
//                            if let identifier = self.uniqueIdentifier(for: $0), trackedObjectIDs.contains(identifier)
//                            {
//                                deletedIDs.append(identifier)
//                            }
                        }
                    }

                    inserted = insertedMutable as? Set<NSManagedObject>

                    var insertedIdentifiersAndEntityNames = [String: String]()
                    inserted?.forEach {
                        if let entityName = $0.entity.name,
                            let identifier = self.uniqueIdentifier(for: $0) {
                            insertedIdentifiersAndEntityNames[identifier] = entityName
                        }
                    }
                    
                    let updatedCount = updatedMutable.count
                    let willHaveChanges = !insertedIdentifiersAndEntityNames.isEmpty || updatedCount > 0 || deletedCount > 0
                    self.privateContext.perform {
                        insertedIdentifiersAndEntityNames.forEach({ (identifier, entityName) in
                            let entity = self.syncedEntity(withOriginIdentifier: identifier)
                            if entity == nil {
                                self.createSyncedEntity(identifier: identifier, entityName: entityName)
                            }
                        })
                        
                        debugPrint("QSCloudKitSynchronizer >> Did Save >> Tracking %ld insertions", inserted?.count ?? 0)

                        self.savePrivateContext()
                        
                        if willHaveChanges {
                            self.hasChanges = true
                            DispatchQueue.main.async {
                                NotificationCenter.default.post(name: .ModelAdapterHasChangesNotification, object: self)
                            }
                        }
                    }
                }
            }
        }
    }
    
    @objc func mncContextNeedsUIRefresh(notification: Notification) {
        
        // convet NSManagedObjectIDs to NSManagedObjects
        let insertedObjectIDs = notification.userInfo?[NSInsertedObjectsKey] as? Set<NSManagedObjectID>
        let insertedMutable = NSMutableSet()
        insertedObjectIDs?.forEach({ (objectID) in
            let managedObject = self.targetContext.object(with:objectID)
            insertedMutable.add(managedObject)
        })
        let inserted = insertedMutable as! Set<NSManagedObject>

        let updatedObjectIDs = notification.userInfo?[NSUpdatedObjectsKey] as? Set<NSManagedObjectID>
        let updatedMutable = NSMutableSet()
        updatedObjectIDs?.forEach({ (objectID) in
            let managedObject = self.targetContext.object(with:objectID)
            updatedMutable.add(managedObject)
        })
        let updated = updatedMutable as! Set<NSManagedObject>
        
        let deletedObjectIDs = notification.userInfo?[NSDeletedObjectsKey] as? Set<NSManagedObjectID>
        let deletedMutable = NSMutableSet()
        deletedObjectIDs?.forEach({ (objectID) in
            let managedObject = self.targetContext.object(with:objectID)
            deletedMutable.add(managedObject)
        })
        let deleted = deletedMutable as! Set<NSManagedObject>

        let notificationWithObjects = Notification(name: .NSManagedObjectContextDidSave, object: notification.object, userInfo: [NSInsertedObjectsKey : inserted, NSUpdatedObjectsKey : updated, NSDeletedObjectsKey : deleted])
        
        self.targetContextDidSave(notification: notificationWithObjects)
    }

    // MARK: sharing
    @objc func targetContextObjectsDidChange(notification: Notification) {
        if isMergingImportedChanges
        {
            debugPrint("targetContextObjectsDidChange.ignore",  notification.userInfo?[NSInsertedObjectsKey] as? Set<NSManagedObject> ?? "no inserted", notification.userInfo?[NSUpdatedObjectsKey] as? Set<NSManagedObject> ?? "no updated", notification.userInfo?[NSDeletedObjectsKey] as? Set<NSManagedObject> ?? "no deleted")
        }
        if let object = notification.object as? NSManagedObjectContext, object == targetContext {
            let updated = notification.userInfo?[NSUpdatedObjectsKey] as? Set<NSManagedObject>
            updated?.forEach {
                let ckOwnerNameKey = "ckOwnerName"
                // fix ckOwnerName changes
                if ($0.changedValuesForCurrentEvent().keys.contains(ckOwnerNameKey))
                {
                    let oldOwner = $0.committedValues(forKeys:[ckOwnerNameKey])[ckOwnerNameKey] as? String ?? ""
                    if (areSharingIdentifiersEqual(oldOwner, sharedZoneOwnerName()) && !areSharingIdentifiersEqual(sharingIdentifier(for: $0), sharedZoneOwnerName()))
                    {
                        // sharing identifier did change
                        let identifierKey = identifierFieldName(forEntity: $0.entity.name!)
                        let oldIdentifier = $0.committedValues(forKeys:[identifierKey])[identifierKey] as? String ?? ""

                        self.privateContext.perform {
                            debugPrint("changedKeys contains owner. mark as new")
                            if let entity = self.syncedEntity(withOriginIdentifier: oldIdentifier)
                            {
                                entity.entityState = .deleted
                            }
                        }
                    }
                }
                
                let primaryKey = "uniqueIdentifier"
                // fix duplicated uniqueIdentifiers
                if (areSharingIdentifiersEqual(sharingIdentifier(for: $0), sharedZoneOwnerName()))
                {
                    if ($0.changedValuesForCurrentEvent().keys.contains(primaryKey))
                    {
                        let oldIdentifier = $0.committedValues(forKeys:[primaryKey])[primaryKey] as? String ?? ""
                        if let identifier = self.uniqueIdentifier(for:$0),
                            (oldIdentifier.count > 0 && identifier.count > 0)
                        {
                            debugPrint("oldIdentifier ", oldIdentifier, "-> newIdentifier ", identifier)
                            self.privateContext.perform {
                                 if let entity = self.syncedEntity(withOriginIdentifier: oldIdentifier) {
                                    entity.entityState = .new
                                    entity.changedKeys = nil
                                    entity.record = nil
                                    entity.identifier = String(format:"%@.%@",entity.entityType ?? "", identifier)
                                    entity.originObjectID = identifier
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    fileprivate func areSharingIdentifiersEqual(_ sharingIdentifier1 : Any?, _ sharingIdentifier2 : Any?) -> Bool {
        let sharingIdentifier1Local : String = sharingIdentifier1 as? String ?? ""
        let sharingIdentifier2Local : String = sharingIdentifier2 as? String ?? ""
        if sharingIdentifier1Local.count == 0 && sharingIdentifier2Local.count == 0
        {
            return true
        }
        return sharingIdentifier1Local == sharingIdentifier2Local
    }

    func sharedZoneOwnerName() -> String?
    {
        if (self.isShared())
        {
            return self.recordZoneID.ownerName
        }
        return nil
    }
    
    func sharingIdentifier(for object:NSManagedObject) -> Any?
    {
        let ckOwnerNameKey = "ckOwnerName"
        return object.value(forKey:ckOwnerNameKey)
    }
}
