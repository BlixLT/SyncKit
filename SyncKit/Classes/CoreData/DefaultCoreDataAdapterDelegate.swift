//
//  DefaultCoreDataAdapterDelegate.swift
//  SyncKit
//
//  Created by Manuel Entrena on 08/06/2019.
//  Copyright © 2019 Manuel Entrena. All rights reserved.
//

import Foundation
import CoreData

public class DefaultCoreDataAdapterDelegate: CoreDataAdapterDelegate {
    public static let shared = DefaultCoreDataAdapterDelegate()
    
    public func coreDataAdapter(_ adapter: CoreDataAdapter, requestsContextSaveWithCompletion completion: (Error?) -> ()) {
        var saveError: Error?
        adapter.targetContext.performAndWait {
            do {
                try adapter.targetContext.save()
            } catch {
                saveError = error
            }
        }
        completion(saveError)
    }
    
    public func coreDataAdapter(_ adapter: CoreDataAdapter, didImportChanges importContext: NSManagedObjectContext, completion: (Error?) -> ()) {
        var saveError: Error?
        adapter.targetContext.performAndWait {
            adapter.targetContext.undoManager?.disableUndoRegistration()
        }
        importContext.performAndWait {
            do {
                try importContext.save()
            } catch {
                saveError = error
            }
        }
        
        if saveError == nil {
            adapter.targetContext.performAndWait {
                adapter.targetContext.processPendingChanges()
                adapter.targetContext.undoManager?.enableUndoRegistration()
                do {
                    try adapter.targetContext.save()
                } catch {
                    saveError = error
                }
            }
        }
        else
        {
            adapter.targetContext.performAndWait {
                adapter.targetContext.processPendingChanges()
                adapter.targetContext.undoManager?.enableUndoRegistration()
            }
        }
        completion(saveError)
    }
    
    public init() { }
}
