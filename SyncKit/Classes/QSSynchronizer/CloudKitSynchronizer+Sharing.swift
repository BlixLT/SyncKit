//
//  CloudKitSynchronizer+Sharing.swift
//  Pods
//
//  Created by Manuel Entrena on 07/04/2019.
//

import Foundation
import CloudKit

@objc public extension CloudKitSynchronizer {
    
    fileprivate func modelAdapter(for object: AnyObject) -> ModelAdapter? {
        for modelAdapter in modelAdapters {
            if modelAdapter.record(for: object) != nil {
                return modelAdapter
            }
        }
        return nil
    }
    
    /**
     Returns the locally stored `CKShare` for a given model object.
     - Parameter object  The model object.
     - Returns: `CKShare` stored for the given object.
     */
    @objc func share(for object: AnyObject) -> CKShare? {
        guard let modelAdapter = modelAdapter(for: object) else {
            return nil
        }
        return modelAdapter.share(for: object)
    }
    
    /**
     Saves the given `CKShare` locally for the given model object.
     - Parameters:
     - share The `CKShare`.
     - object  The model object.
     */
    @objc func saveShare(_ share: CKShare, for object: AnyObject) {
        guard let modelAdapter = modelAdapter(for: object) else {
            return
        }
        modelAdapter.save(share: share, for: object)
    }
    
    /**
     Deletes any `CKShare` locally stored  for the given model object.
     - Parameters:
     - object  The model object.
     */
    @objc func deleteShare(for object: AnyObject) {
        guard let modelAdapter = modelAdapter(for: object) else {
            return
        }
        modelAdapter.deleteShare(for: object)
    }
    
    /**
     Creates and uploads a new `CKShare` for the given model object.
     - Parameters:
     - object The model object to share.
     - publicPermission  The permissions to be used for the new share.
     - participants: The participants to add to this share.
     - completion: Closure that gets called with an optional error when the operation is completed.
     
     */
    @objc func share(object: AnyObject, publicPermission: CKShare.Participant.Permission, extraShareAttributes: Dictionary<String, String>, participants: [CKShare.Participant], completion: ((CKShare?, Error?) -> ())?) {
        
        guard let modelAdapter = modelAdapter(for: object),
            let record = modelAdapter.record(for: object) else {
                return
        }
        let share = CKShare(rootRecord: record)
        share.publicPermission = publicPermission
        for participant in participants {
            share.addParticipant(participant)
        }
        
        addMetadata(to: [record, share])
        
        for (key, value) in extraShareAttributes
        {
            share[key] = value;
        }
        
        let operation = CKModifyRecordsOperation(recordsToSave: [record, share], recordIDsToDelete: nil)
        
        operation.modifyRecordsCompletionBlock = { savedRecords, deletedRecordIDs, operationError in
            
            self.dispatchQueue.async {
                
                let uploadedShare = savedRecords?.first { $0 is CKShare} as? CKShare
                
                if let savedRecords = savedRecords,
                    operationError == nil,
                    let share = uploadedShare {
                    
                    modelAdapter.prepareToImport()
                    let records = savedRecords.filter { $0 != share }
                    modelAdapter.didUpload(savedRecords: records)
                    modelAdapter.persistImportedChanges(completion: { (error) in
                        
                        self.dispatchQueue.async {
                            
                            if error == nil {
                                modelAdapter.save(share: share, for: object)
                            }
                            modelAdapter.didFinishImport(with: error)
                            
                            DispatchQueue.main.async {
                                completion?(uploadedShare, error)
                            }
                        }
                    })
                    
                } else {
                    
                    DispatchQueue.main.async {
                        completion?(uploadedShare, operationError)
                    }
                }
            }
        }
        
        database.add(operation)
    }
    
    /**
     Removes the existing `CKShare` for an object and deletes it from CloudKit.
     - Parameters:
     - object  The model object.
     - completion Closure that gets called on completion.
     */
    @objc func removeShare(for object: AnyObject, completion: ((Error?) -> ())?) {
        
        guard let modelAdapter = modelAdapter(for: object),
            let share = modelAdapter.share(for: object),
            let record = modelAdapter.record(for: object) else {
                completion?(nil)
                return
        }
        
        let operation = CKModifyRecordsOperation(recordsToSave: [record], recordIDsToDelete: [share.recordID])
        operation.modifyRecordsCompletionBlock = { savedRecords, deletedRecordIDs, operationError in
            
            self.dispatchQueue.async {
                
                if let savedRecords = savedRecords,
                    operationError == nil {
                    
                    modelAdapter.prepareToImport()
                    modelAdapter.didUpload(savedRecords: savedRecords)
                    modelAdapter.persistImportedChanges(completion: { (error) in
                        
                        self.dispatchQueue.async {
                            if error == nil {
                                modelAdapter.deleteShare(for: object)
                            }
                            modelAdapter.didFinishImport(with: error)
                            
                            DispatchQueue.main.async {
                                completion?(error)
                            }
                        }
                    })
                    
                } else {
                    
                    DispatchQueue.main.async {
                        completion?(operationError)
                    }
                }
            }
        }
        
        database.add(operation)
    }
    
    /**
     Reuploads to CloudKit all `CKRecord`s for the given root model object and all of its children (see `QSParentKey`). This function can be used to ensure all objects in the hierarchy have their `parent` property correctly set, before sharing, if their records had been created before sharing was supported.
     - Parameters:
     - root The root model object.
     - completion Closure that gets called on completion.
     */
    @objc func reuploadRecordsForChildrenOf(root: AnyObject, completion: ((Error?) -> ())?) {
        
        guard let modelAdapter = modelAdapter(for: root) else {
            completion?(nil)
            return
        }
        
        modelAdapter.recordsToUpdateParentRelationshipsForRoot(root, completion: { (records) in

            guard records.count > 0 else {
                    completion?(nil)
                    return
            }
            
            let operation = CKModifyRecordsOperation(recordsToSave: records, recordIDsToDelete: nil)
            operation.modifyRecordsCompletionBlock = { savedRecords, deletedRecordIDs, operationError in
                
                if operationError == nil,
                    let savedRecords = savedRecords {
                    modelAdapter.didUpload(savedRecords: savedRecords)
                }
                
                completion?(operationError)
            }
            
            self.database.add(operation)

        })
    }
    
    @objc func handleCKShare(_ deletedShare: CKShare, deletionInZone zoneID:CKRecordZone.ID, completion: ((Error?) -> ())?) {
        if self.database.databaseScope == .private
        {
            //private CKShare was deleted;
            // stop sharing extra data to users that are not participating in any "normal" share
            let predicate = NSPredicate(format: "QSCloudKitDeviceUUIDKey != 'sdfarg'")
            let query = CKQuery(recordType:CKRecord.SystemType.share, predicate:predicate)
            let operation = CKQueryOperation(query : query)
            operation.desiredKeys = []
            operation.zoneID = zoneID
            let existingShares = NSMutableArray()
            operation.recordFetchedBlock =  { (record : CKRecord) in
                if record is CKShare {
                    existingShares.add(record)
                }
            }
            operation.completionBlock = {
                let userRecordNamesThatAcceptedAnyShare = NSMutableSet()
                var extraDataShares  = [CKShare]()
                existingShares.forEach { (existingShare) in
                    if !self.isExtraSharedDataShare(share: existingShare as! CKShare)
                    {
                        (existingShare as! CKShare).participants.forEach { (participant) in
                            if participant.acceptanceStatus == .accepted {
                                userRecordNamesThatAcceptedAnyShare.add(participant.userIdentity.userRecordID?.recordName)
                            }
                        }
                    }
                    else
                    {
                        extraDataShares.append(existingShare as! CKShare)
                    }
                }
                let extraDataShare = extraDataShares.last
                
                if extraDataShare!.participants.count > 0
                {
                    let participantsToRemoveFromExtraSharedData = NSMutableSet()
                    let updatedParticipants = NSMutableArray()
                    extraDataShare!.participants.forEach { (participant) in
                        if userRecordNamesThatAcceptedAnyShare.contains(participant.userIdentity.userRecordID?.recordName)
                        {
                            updatedParticipants.add(participant)
                        }
                        else
                        {
                            participantsToRemoveFromExtraSharedData.add(participant)
                        }
                    }
                    if participantsToRemoveFromExtraSharedData.count > 0
                    {
                        var shouldStopSharingExtraData:Bool = false
                        participantsToRemoveFromExtraSharedData.forEach { (participant) in
                            if participant as! CKShare.Participant == extraDataShare!.owner
                            {
                                // current user does not have any accounts shared (currently we does not stop sharing, because otherwise it will take longer for user to share account)
                                // shouldStopSharingExtraData = true
                            }
                            else
                            {
                                extraDataShare!.removeParticipant(participant as! CKShare.Participant)
                            }
                        }
                        if shouldStopSharingExtraData
                        {
                            debugPrint("shouldStopSharingExtraData")  //(currently we does not stop sharing, because otherwise it will take longer for user to share account)
                        }
                        else
                        {
                            debugPrint("should not stopSharingExtraData")  //(currently we does not stop sharing, because otherwise it will take longer for user to share account)
                            self.saveChangesForShare(extraDataShare!, completion: { (share, saveChangesError) in
                                var hasShareLocally : Bool = false
                                self.modelAdapters.forEach {
                                    if $0.hasRecordID(deletedShare.recordID)
                                    {
                                        hasShareLocally = true
                                    }
                                }
                                
                                if hasShareLocally
                                {
                                    DispatchQueue.main.asyncAfter(deadline: DispatchTime.now() + 2) {
                                        self.synchronize(completion: completion)
                                    }
                                }
                                else
                                {
                                    completion!(saveChangesError)
                                }
                            })
                            return
                        }
                    }
                    var hasShareLocally : Bool = false
                    self.modelAdapters.forEach {
                        if $0.hasRecordID(deletedShare.recordID)
                        {
                            hasShareLocally = true
                        }
                    }
                    
                    if hasShareLocally
                    {
                        DispatchQueue.main.asyncAfter(deadline: DispatchTime.now() + 2) {
                            self.synchronize(completion: completion)
                        }
                    }
                    else
                    {
                        completion!(nil)
                    }
                }
                else
                {
                    completion!(nil)
                }
            }
            self.database.add(operation)
        }
        else if self.database.databaseScope == .shared
        {
            debugPrint("handle CKShare deletion in shared db")
            // other owner's share was deleted
            
            let predicate = NSPredicate(format: "QSCloudKitDeviceUUIDKey != 'sdfarg'")
            let query = CKQuery(recordType:CKRecord.SystemType.share, predicate:predicate)
            let operation = CKQueryOperation(query : query)
            operation.desiredKeys = []
            operation.zoneID = zoneID
            let existingShares = NSMutableArray()
            operation.recordFetchedBlock =  { (record : CKRecord) in
                if record is CKShare {
                    existingShares.add(record)
                }
            }
            operation.completionBlock = {
                var extraDataShares  = [CKShare]()
                var hasAcceptedAnyAccountFromRemovedShareOwner : Bool = false
                existingShares.forEach { (existingShare) in
                    if !self.isExtraSharedDataShare(share: existingShare as! CKShare)
                    {
                        (existingShare as! CKShare).participants.forEach { (participant) in
                            if participant == (existingShare as! CKShare).currentUserParticipant {
                                hasAcceptedAnyAccountFromRemovedShareOwner = true
                            }
                        }
                    }
                    else
                    {
                        extraDataShares.append(existingShare as! CKShare)
                    }
                }
                let extraDataShare = extraDataShares.last

                if extraDataShare != nil && !hasAcceptedAnyAccountFromRemovedShareOwner
                {
                    let deleteShareOperation = CKModifyRecordsOperation(recordsToSave: [], recordIDsToDelete: [extraDataShare!.recordID])
                    deleteShareOperation.queuePriority = .high
                    deleteShareOperation.qualityOfService = .userInitiated
                    deleteShareOperation.modifyRecordsCompletionBlock = { savedRecords, deletedRecordIDs, operationError in
                        if deletedRecordIDs!.contains(extraDataShare!.recordID)
                        {
                            debugPrint("extra data share removed successfully remotely")
                        }
                        self.synchronize { (synchronizeSharedDataError) in
                            var hasShareLocally : Bool = false
                            self.modelAdapters.forEach {
                                if $0.hasRecordID(deletedShare.recordID)
                                {
                                    hasShareLocally = true
                                }
                            }
                            
                            if hasShareLocally
                            {
                                DispatchQueue.main.asyncAfter(deadline: DispatchTime.now() + 3) {
                                    self.synchronize(completion: completion)
                                }
                            }
                            else
                            {
                                completion!(synchronizeSharedDataError)
                            }

                        }
                    }
                    self.database.add(deleteShareOperation)
                }
                else
                {
                    self.synchronize { (synchronizeSharedDataError) in
                        var hasShareLocally : Bool = false
                        self.modelAdapters.forEach {
                            if $0.hasRecordID(deletedShare.recordID)
                            {
                                hasShareLocally = true
                            }
                        }
                        
                        if hasShareLocally
                        {
                            DispatchQueue.main.asyncAfter(deadline: DispatchTime.now() + 3) {
                                self.synchronize(completion: completion)
                            }
                        }
                        else
                        {
                            completion!(synchronizeSharedDataError)
                        }
                    }
                }
            }
            self.database.add(operation)
        }
    }

    func isExtraSharedDataShare(share : CKShare) -> Bool
    {
        if share.publicPermission == .readWrite {
            return true
        }
        return false
    }

    func saveChangesForShare(_ object: CKShare, completion: ((CKShare?, Error?) -> ())?) {
        let modifyRecordsOperation = CKModifyRecordsOperation(recordsToSave: [object], recordIDsToDelete : [])
        modifyRecordsOperation.queuePriority = .high
        modifyRecordsOperation.qualityOfService = .userInitiated
        modifyRecordsOperation.modifyRecordsCompletionBlock = { savedRecords, deletedRecordIDs, operationError in
            var savedShares  = [CKShare]()
            savedRecords!.forEach { (record) in
                if record is CKShare
                {
                    savedShares.append(record as! CKShare)
                }
            }
            let share : CKShare = savedShares.first!
            self.dispatchQueue.async {
                completion! (share, operationError)
            }
        }
        self.database.add(modifyRecordsOperation)
    }
}
