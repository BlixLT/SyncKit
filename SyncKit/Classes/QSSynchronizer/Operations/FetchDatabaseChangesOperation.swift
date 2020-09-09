//
//  FetchDatabaseChangesOperation.swift
//  Pods
//
//  Created by Manuel Entrena on 18/05/2018.
//

import Foundation
import CloudKit

public class FetchDatabaseChangesOperation: CloudKitSynchronizerOperation {
    
    let database: CloudKitDatabaseAdapter
    let databaseToken: CKServerChangeToken?
    let completion: (CKServerChangeToken?, [CKRecordZone.ID], [CKRecordZone.ID]) -> ()
    
    var changedZoneIDs = [CKRecordZone.ID]()
    var deletedZoneIDs = [CKRecordZone.ID]()
    weak var internalOperation: CKFetchDatabaseChangesOperation?
    
    public init(database: CloudKitDatabaseAdapter, databaseToken: CKServerChangeToken?, completion: @escaping (CKServerChangeToken?, [CKRecordZone.ID], [CKRecordZone.ID]) -> ()) {
        self.databaseToken = databaseToken
        self.database = database
        self.completion = completion
        super.init()
        debugPrint("FetchDatabaseChangesOperation:", self)
    }
    
    override public func start() {
        debugPrint("FetchDatabaseChangesOperation.start:", self)
        super.start()

        let databaseChangesOperation = CKFetchDatabaseChangesOperation(previousServerChangeToken: databaseToken)
        databaseChangesOperation.fetchAllChanges = true

        databaseChangesOperation.recordZoneWithIDChangedBlock = { zoneID in
            debugPrint("FetchDatabaseChangesOperation.recordZoneWithIDChangedBlock:", self)
            self.changedZoneIDs.append(zoneID)
        }

        databaseChangesOperation.recordZoneWithIDWasDeletedBlock = { zoneID in
            debugPrint("FetchDatabaseChangesOperation.recordZoneWithIDWasDeletedBlock:", self)
            self.deletedZoneIDs.append(zoneID)
        }

        databaseChangesOperation.fetchDatabaseChangesCompletionBlock = { serverChangeToken, moreComing, operationError in

            debugPrint(self.syncPhaseDescription(),"FetchDatabaseChangesOperation.fetchDatabaseChangesCompletionBlock:", self, ", more coming:", moreComing)
            if !moreComing {
                if operationError == nil {
                    self.completion(serverChangeToken, self.changedZoneIDs, self.deletedZoneIDs)
                }

                self.finish(error: operationError)
            }
        }
        
        databaseChangesOperation.completionBlock = {
            debugPrint(self.syncPhaseDescription(),"FetchDatabaseChangesOperation.completionBlock:", self)
        }

        debugPrint(self.syncPhaseDescription(),"FetchDatabaseChangesOperation.will addOperation:", self)

        internalOperation = databaseChangesOperation
        database.add(databaseChangesOperation)
    }
    
    override public func cancel() {
        debugPrint(self.syncPhaseDescription(),"FetchDatabaseChangesOperation.cancel")
        internalOperation?.cancel()
        super.cancel()
    }
    
    func syncPhaseDescription() -> String {
        var sharedOrPrivate = "private"
        if self.database.databaseScope == .shared
        {
            sharedOrPrivate = "shared"
        }
        let syncPhaseDescription = "syncPhase_" + sharedOrPrivate
        return syncPhaseDescription
    }
}
