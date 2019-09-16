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
                let identifier = uniqueIdentifier(for: object)
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
                if changedValueKeys.count > 0 {
                    identifiersAndChanges[identifier] = changedValueKeys
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
                }
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
            let updated = notification.userInfo?[NSUpdatedObjectsKey] as? Set<NSManagedObject>
            let deleted = notification.userInfo?[NSDeletedObjectsKey] as? Set<NSManagedObject>
            
            let insertedMutable = NSMutableSet()
            inserted?.forEach {
                if self.areSharingIdentifiersEqual(self.sharingIdentifier(for: $0), self.sharedZoneOwnerName())
                {
                    insertedMutable.add($0)
                }
            }
            let allUpdateObjectIDs = updated?.map { self.uniqueIdentifier(for: $0) } ?? []
            
            var deletedIDs = deleted?.map { self.uniqueIdentifier(for: $0) } ?? []

            privateContext.perform {
                // get trackedObjectIDs
                let trackedObjects = self.fetchEntities(identifiers:allUpdateObjectIDs)
                let trackedObjectIDs = trackedObjects.map { $0.originObjectID }
                
                self.targetContext.perform {
                    
                    let updatedMutable = NSMutableSet()
                    updated?.forEach {
                        if self.areSharingIdentifiersEqual(self.sharingIdentifier(for: $0), self.sharedZoneOwnerName())
                        {
                            if trackedObjectIDs.contains(self.uniqueIdentifier(for: $0))
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
                            if trackedObjectIDs.contains(self.uniqueIdentifier(for: $0))
                            {
                                deletedIDs.append(self.uniqueIdentifier(for: $0))
                            }
                        }
                    }

                    inserted = insertedMutable as? Set<NSManagedObject>

                    var insertedIdentifiersAndEntityNames = [String: String]()
                    inserted?.forEach {
                        if let entityName = $0.entity.name {
                            insertedIdentifiersAndEntityNames[self.uniqueIdentifier(for: $0)] = entityName
                        }
                    }
                    
                    let updatedIDs = updated?.map { self.uniqueIdentifier(for: $0) } ?? []

                    let willHaveChanges = !insertedIdentifiersAndEntityNames.isEmpty || !updatedIDs.isEmpty || !deletedIDs.isEmpty
                    
                    self.privateContext.perform {
                        insertedIdentifiersAndEntityNames.forEach({ (identifier, entityName) in
                            let entity = self.syncedEntity(withOriginIdentifier: identifier)
                            if entity == nil {
                                self.createSyncedEntity(identifier: identifier, entityName: entityName)
                            }
                        })
                        
                        updatedIDs.forEach({ (identifier) in
                            guard let entity = self.syncedEntity(withOriginIdentifier: identifier) else { return }
                            if entity.entityState == .synced && !entity.changedKeysArray.isEmpty {
                                entity.entityState = .changed
                            }
                            entity.updatedDate = NSDate()
                        })
                        
                        deletedIDs.forEach { (identifier) in
                            guard let entity = self.syncedEntity(withOriginIdentifier: identifier) else {
                                return }
                            entity.entityState = .deleted
                            entity.updatedDate = NSDate()
                        }
                        
                        debugPrint("QSCloudKitSynchronizer >> Tracking %ld insertions", inserted?.count ?? 0)
                        debugPrint("QSCloudKitSynchronizer >> Tracking %ld updates", updated?.count ?? 0)
                        debugPrint("QSCloudKitSynchronizer >> Tracking %ld deletions", deleted?.count ?? 0)
                        
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
                    let oldOwner = $0.committedValues(forKeys:[ckOwnerNameKey])[ckOwnerNameKey] as! String
                    if (areSharingIdentifiersEqual(oldOwner, sharedZoneOwnerName()) && !areSharingIdentifiersEqual(sharingIdentifier(for: $0), sharedZoneOwnerName()))
                    {
                        // sharing identifier did change
                        let identifierKey = identifierFieldName(forEntity: $0.entity.name!)
                        let oldIdentifier = $0.committedValues(forKeys:[identifierKey])[identifierKey] as! String

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
                if ($0.changedValuesForCurrentEvent().keys.contains(primaryKey))
                {
                    let oldIdentifier = $0.committedValues(forKeys:[primaryKey])[primaryKey] as! String
                    let identifier = self.uniqueIdentifier(for:$0)
                    if (oldIdentifier.count > 0 && identifier.count > 0)
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
