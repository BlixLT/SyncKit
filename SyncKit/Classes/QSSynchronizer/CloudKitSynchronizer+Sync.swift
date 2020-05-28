//
//  CloudKitSynchronizer+Sync.swift
//  Pods
//
//  Created by Manuel Entrena on 17/04/2019.
//

import Foundation
import CloudKit

extension CloudKitSynchronizer {
    
    func performSynchronization() {
        debugPrint("performSynchronization")
        dispatchQueue.async {
            self.postNotification(.SynchronizerWillSynchronize)
            self.serverChangeToken = self.storedDatabaseToken
            
            self.modelAdapters.forEach {
                $0.prepareToImport()
            }
            
            self.fetchChanges()
        }
    }
    
    func finishSynchronization(error: Error?) {
        
        resetActiveTokens()
        
        for adapter in modelAdapters {
            adapter.didFinishImport(with: error, clearTempFiles: true)
        }
        
        self.syncing = false
        self.cancelSync = false

        if let error = error {
            self.postNotification(.SynchronizerDidFailToSynchronize, userInfo: [CloudKitSynchronizer.errorKey: error])
        } else {
            self.postNotification(.SynchronizerDidSynchronize)
        }
        
        DispatchQueue.main.async {
            self.completion?(error)
            self.completion = nil
            
            debugPrint("QSCloudKitSynchronizer >> Finishing synchronization:", self)
        }
    }
}

// MARK: - Utilities

extension CloudKitSynchronizer {
    
    func postNotification(_ notification: Notification.Name, object: Any? = self, userInfo: [AnyHashable: Any]? = nil) {
        DispatchQueue.main.async {
            NotificationCenter.default.post(name: notification, object: object, userInfo: userInfo)
        }
    }
    
    func runOperation(_ operation: CloudKitSynchronizerOperation) {
        operation.errorHandler = { [weak self] operation, error in
            self?.finishSynchronization(error: error)
        }
        currentOperation = operation
        operationQueue.addOperation(operation)
    }
    
    func notifyProviderForDeletedZoneIDs(_ zoneIDs: [CKRecordZone.ID]) {
        zoneIDs.forEach {
            self.adapterProvider.cloudKitSynchronizer(self, zoneWasDeletedWithZoneID: $0)
        }
    }
    
    func loadTokens(for zoneIDs: [CKRecordZone.ID], loadAdapters: Bool) -> [CKRecordZone.ID] {
        var filteredZoneIDs = [CKRecordZone.ID]()
        activeZoneTokens = [CKRecordZone.ID: CKServerChangeToken]()
        
        debugPrint("loadTokensFor: ", zoneIDs)
        for zoneID in zoneIDs {
            var modelAdapter = modelAdapterDictionary[zoneID]
            debugPrint("modelAdapter.loadTokens: ", modelAdapter, loadAdapters)
            if modelAdapter == nil && loadAdapters {
                debugPrint("modelAdapterForRecordZoneID: ", zoneID)
                if let newModelAdapter = adapterProvider.cloudKitSynchronizer(self, modelAdapterForRecordZoneID: zoneID) {
                    debugPrint("newModelAdapter: ", newModelAdapter)
                    modelAdapter = newModelAdapter
                    modelAdapterDictionary[zoneID] = newModelAdapter
                    newModelAdapter.prepareToImport()
                }
            }
            
            if let adapter = modelAdapter {
                debugPrint("modelAdapter found")
                filteredZoneIDs.append(zoneID)
                activeZoneTokens[zoneID] = adapter.serverChangeToken
            }
        }
        
        return filteredZoneIDs
    }
    
    func resetActiveTokens() {
        activeZoneTokens = [CKRecordZone.ID: CKServerChangeToken]()
    }
    
    func isServerRecordChangedError(_ error: NSError) -> Bool {
        
        if error.code == CKError.partialFailure.rawValue,
            let errorsByItemID = error.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: NSError],
            errorsByItemID.values.contains(where: { (error) -> Bool in
                return error.code == CKError.serverRecordChanged.rawValue
            }) {
            
            return true
        }
        
        return error.code == CKError.serverRecordChanged.rawValue
    }
    
    func isZoneNotFoundOrDeletedError(_ error: Error?) -> Bool {
        if let error = error {
            let nserror = error as NSError
            return nserror.code == CKError.zoneNotFound.rawValue || nserror.code == CKError.userDeletedZone.rawValue
        } else {
            return false
        }
    }
    
    func isLimitExceededError(_ error: NSError) -> Bool {
        
        if error.code == CKError.partialFailure.rawValue,
            let errorsByItemID = error.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: NSError],
            errorsByItemID.values.contains(where: { (error) -> Bool in
                return error.code == CKError.limitExceeded.rawValue
            }) {
            
            return true
        }
        
        return error.code == CKError.limitExceeded.rawValue
    }
    
    func sequential<T>(objects: [T], closure: @escaping (T, @escaping (Error?)->())->(), final: @escaping  (Error?)->()) {
        
        guard let first = objects.first else {
            final(nil)
            return
        }
        
        closure(first) { error in
            guard error == nil else {
                final(error)
                return
            }
            
            var remaining = objects
            remaining.removeFirst()
            self.sequential(objects: remaining, closure: closure, final: final)
        }
    }
    
    func needsZoneSetup(adapter: ModelAdapter) -> Bool {
        return adapter.serverChangeToken == nil
    }
}

//MARK: - Fetch changes

extension CloudKitSynchronizer {
    
    func fetchChanges() {
        debugPrint("fetchChanges: ", self)
        guard cancelSync == false else {
            finishSynchronization(error: SyncError.cancelled)
            return
        }

        postNotification(.SynchronizerWillFetchChanges)
        fetchDatabaseChanges() { token, error in
            guard error == nil else {
                self.finishSynchronization(error: error)
                return
            }
            
            self.serverChangeToken = token
            self.storedDatabaseToken = token
            if self.syncMode == .sync {
                self.uploadChanges()
            } else {
                self.finishSynchronization(error: nil)
            }
        }
    }
    
    func fetchDatabaseChanges(completion: @escaping (CKServerChangeToken?, Error?) -> ()) {
        
        debugPrint("fetchDatabaseChanges: ", self)
        let operation = FetchDatabaseChangesOperation(database: database, databaseToken: serverChangeToken) { (token, changedZoneIDs, deletedZoneIDs) in
            debugPrint("fetchDatabaseChanges.completion: ", self)
            self.dispatchQueue.async {
                self.notifyProviderForDeletedZoneIDs(deletedZoneIDs)
                
                let zoneIDsToFetch = self.loadTokens(for: changedZoneIDs, loadAdapters: true)
                
                guard zoneIDsToFetch.count > 0 else {
                    debugPrint("zoneIDsToFetch.count == 0", "changedZoneIDs", changedZoneIDs)
                    self.resetActiveTokens()
                    completion(token, nil)
                    return
                }
                
                self.fetchZoneChanges(zoneIDsToFetch) { error in
                    guard error == nil else {
                        self.finishSynchronization(error: error)
                        return
                    }
                    
                    self.mergeChanges() { error in
                        completion(token, error)
                    }
                }
            }
        }
        
        runOperation(operation)
    }
    
    func fetchZoneChanges(_ zoneIDs: [CKRecordZone.ID], completion: @escaping (Error?)->()) {
        debugPrint("fetchZoneChanges: ", self)
        var ignoreDeviceIdentifier = " "
        // ignore deviceIdentifier in shared db (doesn't work here - after owner unshares and shares again some object, it might have last change from this device)
        if database.databaseScope == .private
        {
            ignoreDeviceIdentifier = deviceIdentifier
        }
        let operation = FetchZoneChangesOperation(database: database, zoneIDs: zoneIDs, zoneChangeTokens: activeZoneTokens, modelVersion: compatibilityVersion, ignoreDeviceIdentifier: ignoreDeviceIdentifier, desiredKeys: nil) { (zoneResults) in
            
            debugPrint("fetchZoneChanges.completion: ", self)
            self.dispatchQueue.async {
                var pendingZones = [CKRecordZone.ID]()
                var error: Error? = nil
                
                for (zoneID, result) in zoneResults {
                    let adapter = self.modelAdapterDictionary[zoneID]
                    if let resultError = result.error {
                        if self.isZoneNotFoundOrDeletedError(resultError)
                        {
                            debugPrint("QSCloudKitSynchronizer.fetchZoneChanges >> got zone not found error")
                            self.notifyProviderForDeletedZoneIDs([zoneID])
                        }
                        else
                        {
                            error = resultError
                            break
                        }
                    } else {
                        debugPrint("QSCloudKitSynchronizer >> Downloaded \(result.downloadedRecords.count) changed records >> from zone \(zoneID.description)")
                        debugPrint("QSCloudKitSynchronizer >> Downloaded \(result.deletedRecordIDs.count) deleted record IDs >> from zone \(zoneID.description)")
                        self.activeZoneTokens[zoneID] = result.serverChangeToken
                        adapter?.saveChanges(in: result.downloadedRecords)
                        adapter?.deleteRecords(with: result.deletedRecordIDs)
                        if result.moreComing {
                            pendingZones.append(zoneID)
                        }
                    }
                }
                
                if pendingZones.count > 0 && error == nil {
                    debugPrint("fetchZoneChanges.pendingZones.count > 0:", self)
                    self.fetchZoneChanges(pendingZones, completion: completion)
                } else {
                    completion(error)
                }
            }
        }
        runOperation(operation)
    }
    
    func mergeChanges(completion: @escaping (Error?)->()) {
        debugPrint("mergeChanges:", self)
        guard cancelSync == false else {
            finishSynchronization(error: SyncError.cancelled)
            return
        }
        
        var adapterSet = [ModelAdapter]()
        activeZoneTokens.keys.forEach {
            if let adapter = self.modelAdapterDictionary[$0] {
                adapterSet.append(adapter)
            }
        }

        sequential(objects: adapterSet, closure: mergeChangesIntoAdapter, final: completion)
    }
    
    func mergeChangesIntoAdapter(_ adapter: ModelAdapter, completion: @escaping (Error?)->()) {

        adapter.persistImportedChanges { error in
            self.dispatchQueue.async {
                guard error == nil else {
                    completion(error)
                    return
                }
                
                adapter.saveToken(self.activeZoneTokens[adapter.recordZoneID])
                completion(nil)
            }
        }
    }
}

// MARK: - Upload changes

extension CloudKitSynchronizer {
    
    func uploadChanges() {
        debugPrint("uploadChanges")
        guard cancelSync == false else {
            finishSynchronization(error: SyncError.cancelled)
            return
        }

        postNotification(.SynchronizerWillUploadChanges)
        
        uploadChanges() { (error) in
            if let error = error {
                if self.isServerRecordChangedError(error as NSError) {
                    self.handleServerRecordChangedError(serverRecordsChangedError:error as NSError) {
                        (error) in
                        self.fetchChanges()
                    }
                } else {
                    self.finishSynchronization(error: error)
                }
            } else {
                self.updateTokens()
            }
        }
    }
    
    func handleServerRecordChangedError(serverRecordsChangedError:NSError, completion: @escaping (Error?)->()) {
        
        let errorsByItemID = serverRecordsChangedError.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: NSError]
        var serverRecordChangedRecordIDs : [CKRecord.ID] = [CKRecord.ID] ()
        errorsByItemID!.forEach { (ckrecordid, error) in
            if error.code == CKError.serverRecordChanged.rawValue {
                serverRecordChangedRecordIDs.append(ckrecordid)
            }
        }
        
        if serverRecordChangedRecordIDs.count > 0 {
            let fetchRecordsOperation = CKFetchRecordsOperation(recordIDs: serverRecordChangedRecordIDs)
            fetchRecordsOperation.fetchRecordsCompletionBlock = { recordsByID, fetchRecordsError in
                    
                let fetchedRecords = Array(recordsByID!.values)
                if fetchedRecords.count == 0
                {
                    debugPrint("no records downloaded for handleServerRecordChangedError: ", serverRecordsChangedError)
                }
                else
                {
                    debugPrint("ServerRecordChanged records downloaded: ", fetchedRecords.count)
                }
                if let resultError = fetchRecordsError {
                    completion(resultError)
                } else {
                    var recordsByZoneID = [CKRecordZone.ID : [CKRecord]]()
                    fetchedRecords.forEach { ckrecord in
                        let zoneID = ckrecord.recordID.zoneID
                        
                        // get existing items, or create new array if doesn't exist
                        var existingItems = recordsByZoneID[zoneID] ?? [CKRecord]()
                        // append the item
                        existingItems.append(ckrecord)
                        // replace back into `data`
                        recordsByZoneID[zoneID] = existingItems
                    }
                    
                    recordsByZoneID.forEach { (zoneID, records) in
                        let adapter = self.modelAdapterDictionary[zoneID]
                        adapter?.saveChanges(in: records)
                    }
                    completion(nil)
                }
            }
            database.add(fetchRecordsOperation)
        }
        else
        {
            completion(nil)
        }
    }
    
    func uploadChanges(completion: @escaping (Error?)->()) {
        sequential(objects: modelAdapters, closure: setupZoneAndUploadRecords) { (error) in
            guard error == nil else { completion(error); return }
            
            self.sequential(objects: self.modelAdapters, closure: self.uploadDeletions, final: completion)
        }
    }
    
    func setupZoneAndUploadRecords(adapter: ModelAdapter, completion: @escaping (Error?)->()) {
        setupRecordZoneIfNeeded(adapter: adapter) { (error) in
            
            guard error == nil else { completion(error); return }
            
            self.uploadRecords(adapter: adapter, completion: { (error) in
                guard error == nil else { completion(error); return }
                
                completion(nil)
            })
        }
    }
    
    func setupRecordZoneIfNeeded(adapter: ModelAdapter, completion: @escaping (Error?)->()) {
        guard needsZoneSetup(adapter: adapter) else {
            completion(nil)
            return
        }
        
        setupRecordZoneID(adapter.recordZoneID, completion: completion)
    }
    
    func setupRecordZoneID(_ zoneID: CKRecordZone.ID, completion: @escaping (Error?)->()) {
        database.fetch(withRecordZoneID: zoneID) { (zone, error) in
            if self.isZoneNotFoundOrDeletedError(error) {
                let newZone = CKRecordZone(zoneID: zoneID)
                self.database.save(zone: newZone, completionHandler: { (zone, error) in
                    if error == nil && zone != nil {
                        debugPrint("QSCloudKitSynchronizer >> Created custom record zone: \(newZone.description)")
                    }
                    completion(error)
                })
            } else {
                completion(error)
            }
        }
    }
    
    func uploadRecords(adapter: ModelAdapter,  completion: @escaping (Error?)->()) {
        let records = adapter.recordsToUpload(limit: batchSize)
        let recordCount = records.count
        let requestedBatchSize = batchSize
        guard recordCount > 0 else { completion(nil); return }
        
        //Add metadata: device UUID and model version
        addMetadata(to: records)
        
        let modifyRecordsOperation = CKModifyRecordsOperation(recordsToSave: records, recordIDsToDelete: nil)
        
        debugPrint("will create modifyRecordsOperation ", adapter.recordZoneID)
        modifyRecordsOperation.modifyRecordsCompletionBlock = { savedRecords, deletedRecordIDs, operationError in
            
            self.dispatchQueue.async {
                
                debugPrint("modifyRecordsCompletionBlock.error:", operationError ?? "nil", adapter.recordZoneID)
                if let error = operationError {
                    debugPrint("(error) successfully uploaded records:", savedRecords?.count ?? 0, "deleted records:", deletedRecordIDs?.count ?? 0)

                    if self.isLimitExceededError(error as NSError) {
                        self.batchSize = self.batchSize / 2
                    }
                    completion(error)
                } else {
                    if self.batchSize < CloudKitSynchronizer.defaultBatchSize {
                        self.batchSize = self.batchSize + 5
                    }
                    
                    adapter.didUpload(savedRecords: savedRecords ?? [])
                    
                    debugPrint("QSCloudKitSynchronizer >> Uploaded \(savedRecords?.count ?? 0) records")
                    
                    if recordCount >= requestedBatchSize {
                        self.uploadRecords(adapter: adapter, completion: completion)
                    } else {
                        completion(nil)
                    }
                }
            }
        }
        
        currentOperation = modifyRecordsOperation
        database.add(modifyRecordsOperation)
    }
    
    func uploadDeletions(adapter: ModelAdapter, completion: @escaping (Error?)->()) {
        
        let recordIDs = adapter.recordIDsMarkedForDeletion(limit: batchSize)
        let recordCount = recordIDs.count

        guard recordCount > 0 else {
            completion(nil)
            return
        }
        
        let modifyRecordsOperation = CKModifyRecordsOperation(recordsToSave: nil, recordIDsToDelete: recordIDs)
        modifyRecordsOperation.modifyRecordsCompletionBlock = { savedRecords, deletedRecordIDs, operationError in
            self.dispatchQueue.async {
                debugPrint("QSCloudKitSynchronizer >> Deleted \(recordCount) records")
                
                if let error = operationError,
                    self.isLimitExceededError(error as NSError) {
                        
                    self.batchSize = self.batchSize / 2
                } else if self.batchSize < CloudKitSynchronizer.defaultBatchSize {
                    self.batchSize = self.batchSize + 5
                }
                
                adapter.didDelete(recordIDs: deletedRecordIDs ?? [])
                
                completion(operationError)
            }
        }
        
        currentOperation = modifyRecordsOperation
        database.add(modifyRecordsOperation)
    }
    
    // MARK: - 
    
    func updateTokens() {
        let operation = FetchDatabaseChangesOperation(database: database, databaseToken: serverChangeToken) { (databaseToken, changedZoneIDs, deletedZoneIDs) in
            self.dispatchQueue.async {
                self.notifyProviderForDeletedZoneIDs(deletedZoneIDs)
                debugPrint("updateTokens. changedZoneIDs: ", changedZoneIDs)
                if changedZoneIDs.count > 0 {
                    let zoneIDs = self.loadTokens(for: changedZoneIDs, loadAdapters: false)
                    self.updateServerToken(for: zoneIDs, completion: { (needsToFetchChanges) in
                        if needsToFetchChanges {
                            self.performSynchronization()
                        } else {
                            self.storedDatabaseToken = databaseToken
                            self.finishSynchronization(error: nil)
                        }
                    })
                } else {
                    self.finishSynchronization(error: nil)
                }
            }
        }
        runOperation(operation)
    }
    
    func updateServerToken(for recordZoneIDs: [CKRecordZone.ID], completion: @escaping (Bool)->()) {
        
        // If we found a new record zone at this point then needsToFetchChanges=true
        debugPrint("updateServerToken")
        var hasAllTokens = true
        for zoneID in recordZoneIDs {
            if activeZoneTokens[zoneID] == nil {
                hasAllTokens = false
            }
        }
        guard hasAllTokens else {
            completion(true)
            return
        }
        
        let operation = FetchZoneChangesOperation(database: database, zoneIDs: recordZoneIDs, zoneChangeTokens: activeZoneTokens, modelVersion: compatibilityVersion, ignoreDeviceIdentifier: deviceIdentifier, desiredKeys: ["recordID", CloudKitSynchronizer.deviceUUIDKey]) { (zoneResults) in
            self.dispatchQueue.async {
                var pendingZones = [CKRecordZone.ID]()
                var needsToRefetch = false
                
                for (zoneID, result) in zoneResults {
                    let adapter = self.modelAdapterDictionary[zoneID]
                    if result.downloadedRecords.count > 0 || result.deletedRecordIDs.count > 0 {
                        debugPrint("needsToRefetch")
                        needsToRefetch = true
                    } else {
                        self.activeZoneTokens[zoneID] = result.serverChangeToken
                        adapter?.saveToken(result.serverChangeToken)
                    }
                    if result.moreComing {
                        pendingZones.append(zoneID)
                    }
                }
                
                if pendingZones.count > 0 && !needsToRefetch {
                    self.updateServerToken(for: pendingZones, completion: completion)
                } else {
                    completion(needsToRefetch)
                }
            }
        }
        runOperation(operation)
    }
}
