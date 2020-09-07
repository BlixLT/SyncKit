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
    
    public func coreDataAdapter(_ adapter: CoreDataAdapter, requestsContextSaveWithCompletion completion: @escaping (Error?) -> ()) {
        var saveError: Error?
        adapter.targetContext.perform {
            do {
                try adapter.targetContext.save()
            } catch {
                saveError = error
            }
            completion(saveError)
        }
    }
    
    public func coreDataAdapter(_ adapter: CoreDataAdapter, didImportChanges importContext: NSManagedObjectContext, completion: @escaping (Error?) -> ()) {
        var saveError: Error?
        importContext.perform {
            do {
                try importContext.save()
            } catch {
                saveError = error
            }
            completion(saveError)
        }
    }
    
    public init() { }
}
