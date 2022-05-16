//
//  QSNamesTransformer.swift
//  SyncKitCoreDataExampleTests
//
//  Created by Manuel Entrena on 18/06/2019.
//  Copyright © 2019 Manuel Entrena. All rights reserved.
//

import Foundation

class QSNamesSecureUnarchiverTransformer: NSSecureUnarchiveFromDataTransformer {

    static var transformedValueCalled = false
    static var reverseTransformedValueCalled = false
    static func resetValues() {
        transformedValueCalled = false
        reverseTransformedValueCalled = false
    }
    
    static func register() {
        ValueTransformer.setValueTransformer(QSNamesSecureUnarchiverTransformer(), forName: .namesSecureUnarchiverTransformerName)
    }
    
    override class func transformedValueClass() -> AnyClass {
        NSData.self
    }

    override class func allowsReverseTransformation() -> Bool {
        true
    }
    
    override func transformedValue(_ value: Any?) -> Any? {
        QSNamesSecureUnarchiverTransformer.transformedValueCalled = true
        guard let data = value as? Data else {
            return nil
        }
        return try! NSKeyedUnarchiver.unarchivedObject(ofClasses:[NSString.self, NSArray.self], from: data)
    }
    
    override func reverseTransformedValue(_ value: Any?) -> Any? {
        QSNamesSecureUnarchiverTransformer.reverseTransformedValueCalled = true
        return try! NSKeyedArchiver.archivedData(withRootObject: value, requiringSecureCoding:true)
    }
}

extension NSValueTransformerName {
    static let namesSecureUnarchiverTransformerName = NSValueTransformerName(rawValue: "QSNamesSecureUnarchiverTransformer")
}
