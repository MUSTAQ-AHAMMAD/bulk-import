"""
================================================================================
ORACLE FUSION FINANCIAL INTEGRATION MODULE (FINAL - EXACT HEADER MATCHING)
================================================================================
This module generates AR Invoice Import Template with EXACT column headers
as per the FBDA template specification.
================================================================================
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
import warnings
import re
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional, Any, Tuple

warnings.filterwarnings('ignore')


class OracleFusionIntegration:
    
    def __init__(self, base_output_dir: str = "ORACLE_FUSION_OUTPUT", transaction_prefix: str = "BULK-ALAJH", starting_sequence: int = 1):
        self.base_output_dir = base_output_dir
        self.transaction_prefix = transaction_prefix
        self.starting_sequence = starting_sequence
        Path(self.base_output_dir).mkdir(parents=True, exist_ok=True)
        
        # =====================================================================
        # EXACT AR INVOICE COLUMN HEADERS (as per FBDA template)
        # =====================================================================
        self.ar_columns = self._get_exact_ar_columns()
        
        # Static values for AR invoices
        self.ar_static_fields = {
            'Transaction Batch Source Name': 'Manual_Imported',
            'Transaction Type Name': 'Vend Invoice',
            'Payment Terms': 'IMMEDIATE',
            'Transaction Line Type': 'LINE',
            'Currency Code': 'SAR',
            'Currency Conversion Type': 'Corporate',
            'Currency Conversion Rate': '1',
            'Line Transactions Flexfield Context': 'Legacy',
            'Unit of Measure Code': 'Ea',
            'Default Taxation Country': 'SA',
            'END': 'END',
            'Comments': 'AlQurashi-KSA'
        }
        
        # Payment methods that get receipts
        self.receipt_payment_methods = {'Cash', 'Card', 'Mada', 'Visa', 'MasterCard', 'Amex'}
        
        self.payment_method_bank_mapping = {
            'Cash': ('Cash', 'Cash Account'),
            'Card': ('Card', 'Card Account'),
            'Mada': ('Mada', 'Mada Account'),
            'Visa': ('Visa', 'Visa Account'),
            'MasterCard': ('MasterCard', 'MC Account'),
            'Amex': ('Amex', 'Amex Account'),
        }
        
        self.payment_method_normalization = {
            'CASH': 'Cash',
            'CARD': 'Card',
            'MADA': 'Mada',
            'VISA': 'Visa',
            'MASTERCARD': 'MasterCard',
            'AMEX': 'Amex',
            'AMERICAN EXPRESS': 'Amex',
            'TAMARA': 'TAMARA',
            'TABBY': 'TABBY',
        }
        
        self.default_bank = ('Cash', 'Cash Account')
        
        # =====================================================================
        # PROCESSING STATE VARIABLES
        # =====================================================================
        self.ar_transaction_counter = 1
        self.ar_segment_counter = 1
        self.invoice_to_ar_transaction = {}
        
        self.customer_type_cache = {}
        self.subinventory_to_customer_type = {}
        self.register_customer_cache = {}
        
        self.invoice_payment_map = defaultdict(lambda: defaultdict(float))
        self.invoice_store_map = {}
        self.invoice_register_map = {}
        self.invoice_customer_type = {}
        self.transaction_number_map = {}
        self.last_transaction_number = starting_sequence - 1
        self.generation_stats = {}
        
        self.line_items = None
        self.payments = None
        self.metadata = None
        self.registers = None
        
        self.validation_data = []
        
    # ========================================================================
    # EXACT COLUMN HEADERS - MATCHING YOUR TEMPLATE EXACTLY
    # ========================================================================
    
    def _get_exact_ar_columns(self) -> List[str]:
        """Return EXACT AR invoice column headers as per FBDA template."""
        return [
            'ID',
            'Transaction Batch Source Name',
            'Transaction Type Name',
            'Payment Terms',
            'Transaction Date',
            'Accounting Date',
            'Transaction Number',
            'Original System Bill-to Customer Reference',
            'Original System Bill-to Customer Address Reference',
            'Original System Bill-to Customer Contact Reference',
            'Original System Ship-to Customer Reference',
            'Original System Ship-to Customer Address Reference',
            'Original System Ship-to Customer Contact Reference',
            'Original System Ship-to Customer Account Reference',
            'Original System Ship-to Customer Account Address Reference',
            'Original System Ship-to Customer Account Contact Reference',
            'Original System Sold-to Customer Reference',
            'Original System Sold-to Customer Account Reference',
            'Bill-to Customer Account Number',
            'Bill-to Customer Site Number',
            'Bill-to Contact Party Number',
            'Ship-to Customer Account Number',
            'Ship-to Customer Site Number',
            'Ship-to Contact Party Number',
            'Sold-to Customer Account Number',
            'Transaction Line Type',
            'Transaction Line Description',
            'Currency Code',
            'Currency Conversion Type',
            'Currency Conversion Date',
            'Currency Conversion Rate',
            'Transaction Line Amount',
            'Transaction Line Quantity',
            'Customer Ordered Quantity',
            'Unit Selling Price',
            'Unit Standard Price',
            'Line Transactions Flexfield Context',
            'Line Transactions Flexfield Segment 1',
            'Line Transactions Flexfield Segment 2',
            'Line Transactions Flexfield Segment 3',
            'Line Transactions Flexfield Segment 4',
            'Line Transactions Flexfield Segment 5',
            'Line Transactions Flexfield Segment 6',
            'Line Transactions Flexfield Segment 7',
            'Line Transactions Flexfield Segment 8',
            'Line Transactions Flexfield Segment 9',
            'Line Transactions Flexfield Segment 10',
            'Line Transactions Flexfield Segment 11',
            'Line Transactions Flexfield Segment 12',
            'Line Transactions Flexfield Segment 13',
            'Line Transactions Flexfield Segment 14',
            'Line Transactions Flexfield Segment 15',
            'Primary Salesperson Number',
            'Tax Classification Code',
            'Legal Entity Identifier',
            'Accounted Amount in Ledger Currency',
            'Sales Order Number',
            'Sales Order Date',
            'Actual Ship Date',
            'Warehouse Code',
            'Unit of Measure Code',
            'Unit of Measure Name',
            'Invoicing Rule Name',
            'Revenue Scheduling Rule Name',
            'Number of Revenue Periods',
            'Revenue Scheduling Rule Start Date',
            'Revenue Scheduling Rule End Date',
            'Reason Code Meaning',
            'Last Period to Credit',
            'Transaction Business Category Code',
            'Product Fiscal Classification Code',
            'Product Category Code',
            'Product Type',
            'Line Intended Use Code',
            'Assessable Value',
            'Document Sub Type',
            'Default Taxation Country',
            'User Defined Fiscal Classification',
            'Tax Invoice Number',
            'Tax Invoice Date',
            'Tax Regime Code',
            'Tax',
            'Tax Status Code',
            'Tax Rate Code',
            'Tax Jurisdiction Code',
            'First Party Registration Number',
            'Third Party Registration Number',
            'Final Discharge Location',
            'Taxable Amount',
            'Taxable Flag',
            'Tax Exemption Flag',
            'Tax Exemption Reason Code',
            'Tax Exemption Reason Code Meaning',
            'Tax Exemption Certificate Number',
            'Line Amount Includes Tax Flag',
            'Tax Precedence',
            'Credit Method To Be Used For Lines With Revenue Scheduling Rules',
            'Credit Method To Be Used For Transactions With Split Payment Terms',
            'Reason Code',
            'Tax Rate',
            'FOB Point',
            'Carrier',
            'Shipping Reference',
            'Sales Order Line Number',
            'Sales Order Source',
            'Sales Order Revision Number',
            'Purchase Order Number',
            'Purchase Order Revision Number',
            'Purchase Order Date',
            'Agreement Name',
            'Memo Line Name',
            'Document Number',
            'Original System Batch Name',
            'Link-to Transactions Flexfield Context',
            'Link-to Transactions Flexfield Segment 1',
            'Link-to Transactions Flexfield Segment 2',
            'Link-to Transactions Flexfield Segment 3',
            'Link-to Transactions Flexfield Segment 4',
            'Link-to Transactions Flexfield Segment 5',
            'Link-to Transactions Flexfield Segment 6',
            'Link-to Transactions Flexfield Segment 7',
            'Link-to Transactions Flexfield Segment 8',
            'Link-to Transactions Flexfield Segment 9',
            'Link-to Transactions Flexfield Segment 10',
            'Link-to Transactions Flexfield Segment 11',
            'Link-to Transactions Flexfield Segment 12',
            'Link-to Transactions Flexfield Segment 13',
            'Link-to Transactions Flexfield Segment 14',
            'Link-to Transactions Flexfield Segment 15',
            'Reference Transactions Flexfield Context',
            'Reference Transactions Flexfield Segment 1',
            'Reference Transactions Flexfield Segment 2',
            'Reference Transactions Flexfield Segment 3',
            'Reference Transactions Flexfield Segment 4',
            'Reference Transactions Flexfield Segment 5',
            'Reference Transactions Flexfield Segment 6',
            'Reference Transactions Flexfield Segment 7',
            'Reference Transactions Flexfield Segment 8',
            'Reference Transactions Flexfield Segment 9',
            'Reference Transactions Flexfield Segment 10',
            'Reference Transactions Flexfield Segment 11',
            'Reference Transactions Flexfield Segment 12',
            'Reference Transactions Flexfield Segment 13',
            'Reference Transactions Flexfield Segment 14',
            'Reference Transactions Flexfield Segment 15',
            'Link To Parent Line Context',
            'Link To Parent Line Segment 1',
            'Link To Parent Line Segment 2',
            'Link To Parent Line Segment 3',
            'Link To Parent Line Segment 4',
            'Link To Parent Line Segment 5',
            'Link To Parent Line Segment 6',
            'Link To Parent Line Segment 7',
            'Link To Parent Line Segment 8',
            'Link To Parent Line Segment 9',
            'Link To Parent Line Segment 10',
            'Link To Parent Line Segment 11',
            'Link To Parent Line Segment 12',
            'Link To Parent Line Segment 13',
            'Link To Parent Line Segment 14',
            'Link To Parent Line Segment 15',
            'Receipt Method Name',
            'Printing Option',
            'Related Batch Source Name',
            'Related Transaction Number',
            'Inventory Item Number',
            'Inventory Item Segment 2',
            'Inventory Item Segment 3',
            'Inventory Item Segment 4',
            'Inventory Item Segment 5',
            'Inventory Item Segment 6',
            'Inventory Item Segment 7',
            'Inventory Item Segment 8',
            'Inventory Item Segment 9',
            'Inventory Item Segment 10',
            'Inventory Item Segment 11',
            'Inventory Item Segment 12',
            'Inventory Item Segment 13',
            'Inventory Item Segment 14',
            'Inventory Item Segment 15',
            'Inventory Item Segment 16',
            'Inventory Item Segment 17',
            'Inventory Item Segment 18',
            'Inventory Item Segment 19',
            'Inventory Item Segment 20',
            'Bill To Customer Bank Account Name',
            'Reset Transaction Date Flag',
            'Payment Server Order Number',
            'Last Transaction on Debit Authorization',
            'Approval Code',
            'Address Verification Code',
            'Transaction Line Translated Description',
            'Consolidated Billing Number',
            'Promised Commitment Amount',
            'Payment Set Identifier',
            'Original Accounting Date',
            'Invoiced Line Accounting Level',
            'Override AutoAccounting Flag',
            'Historical Flag',
            'Deferral Exclusion Flag',
            'Payment Attributes',
            'Invoice Billing Date',
            'Invoice Lines Flexfield Context',
            'Invoice Lines Flexfield Segment 1',
            'Invoice Lines Flexfield Segment 2',
            'Invoice Lines Flexfield Segment 3',
            'Invoice Lines Flexfield Segment 4',
            'Invoice Lines Flexfield Segment 5',
            'Invoice Lines Flexfield Segment 6',
            'Invoice Lines Flexfield Segment 7',
            'Invoice Lines Flexfield Segment 8',
            'Invoice Lines Flexfield Segment 9',
            'Invoice Lines Flexfield Segment 10',
            'Invoice Lines Flexfield Segment 11',
            'Invoice Lines Flexfield Segment 12',
            'Invoice Lines Flexfield Segment 13',
            'Invoice Lines Flexfield Segment 14',
            'Invoice Lines Flexfield Segment 15',
            'Invoice Transactions Flexfield Context',
            'Invoice Transactions Flexfield Segment 1',
            'Invoice Transactions Flexfield Segment 2',
            'Invoice Transactions Flexfield Segment 3',
            'Invoice Transactions Flexfield Segment 4',
            'Invoice Transactions Flexfield Segment 5',
            'Invoice Transactions Flexfield Segment 6',
            'Invoice Transactions Flexfield Segment 7',
            'Invoice Transactions Flexfield Segment 8',
            'Invoice Transactions Flexfield Segment 9',
            'Invoice Transactions Flexfield Segment 10',
            'Invoice Transactions Flexfield Segment 11',
            'Invoice Transactions Flexfield Segment 12',
            'Invoice Transactions Flexfield Segment 13',
            'Invoice Transactions Flexfield Segment 14',
            'Invoice Transactions Flexfield Segment 15',
            'Receivables Transaction Region Information Flexfield Context',
            'Receivables Transaction Region Information Flexfield Segment 1',
            'Receivables Transaction Region Information Flexfield Segment 2',
            'Receivables Transaction Region Information Flexfield Segment 3',
            'Receivables Transaction Region Information Flexfield Segment 4',
            'Receivables Transaction Region Information Flexfield Segment 5',
            'Receivables Transaction Region Information Flexfield Segment 6',
            'Receivables Transaction Region Information Flexfield Segment 7',
            'Receivables Transaction Region Information Flexfield Segment 8',
            'Receivables Transaction Region Information Flexfield Segment 9',
            'Receivables Transaction Region Information Flexfield Segment 10',
            'Receivables Transaction Region Information Flexfield Segment 11',
            'Receivables Transaction Region Information Flexfield Segment 12',
            'Receivables Transaction Region Information Flexfield Segment 13',
            'Receivables Transaction Region Information Flexfield Segment 14',
            'Receivables Transaction Region Information Flexfield Segment 15',
            'Receivables Transaction Region Information Flexfield Segment 16',
            'Receivables Transaction Region Information Flexfield Segment 17',
            'Receivables Transaction Region Information Flexfield Segment 18',
            'Receivables Transaction Region Information Flexfield Segment 19',
            'Receivables Transaction Region Information Flexfield Segment 20',
            'Receivables Transaction Region Information Flexfield Segment 21',
            'Receivables Transaction Region Information Flexfield Segment 22',
            'Receivables Transaction Region Information Flexfield Segment 23',
            'Receivables Transaction Region Information Flexfield Segment 24',
            'Receivables Transaction Region Information Flexfield Segment 25',
            'Receivables Transaction Region Information Flexfield Segment 26',
            'Receivables Transaction Region Information Flexfield Segment 27',
            'Receivables Transaction Region Information Flexfield Segment 28',
            'Receivables Transaction Region Information Flexfield Segment 29',
            'Receivables Transaction Region Information Flexfield Segment 30',
            'Line Global Descriptive Flexfield Attribute Category',
            'Line Global Descriptive Flexfield Segment 1',
            'Line Global Descriptive Flexfield Segment 2',
            'Line Global Descriptive Flexfield Segment 3',
            'Line Global Descriptive Flexfield Segment 4',
            'Line Global Descriptive Flexfield Segment 5',
            'Line Global Descriptive Flexfield Segment 6',
            'Line Global Descriptive Flexfield Segment 7',
            'Line Global Descriptive Flexfield Segment 8',
            'Line Global Descriptive Flexfield Segment 9',
            'Line Global Descriptive Flexfield Segment 10',
            'Line Global Descriptive Flexfield Segment 11',
            'Line Global Descriptive Flexfield Segment 12',
            'Line Global Descriptive Flexfield Segment 13',
            'Line Global Descriptive Flexfield Segment 14',
            'Line Global Descriptive Flexfield Segment 15',
            'Line Global Descriptive Flexfield Segment 16',
            'Line Global Descriptive Flexfield Segment 17',
            'Line Global Descriptive Flexfield Segment 18',
            'Line Global Descriptive Flexfield Segment 19',
            'Line Global Descriptive Flexfield Segment 20',
            'Comments',
            'Notes from Source',
            'Credit Card Token Number',
            'Credit Card Expiration Date',
            'First Name of the Credit Card Holder',
            'Last Name of the Credit Card Holder',
            'Credit Card Issuer Code',
            'Masked Credit Card Number',
            'Credit Card Authorization Request Identifier',
            'Credit Card Voice Authorization Code',
            'Receivables Transaction Region Information Flexfield Number Segment 1',
            'Receivables Transaction Region Information Flexfield Number Segment 2',
            'Receivables Transaction Region Information Flexfield Number Segment 3',
            'Receivables Transaction Region Information Flexfield Number Segment 4',
            'Receivables Transaction Region Information Flexfield Number Segment 5',
            'Receivables Transaction Region Information Flexfield Number Segment 6',
            'Receivables Transaction Region Information Flexfield Number Segment 7',
            'Receivables Transaction Region Information Flexfield Number Segment 8',
            'Receivables Transaction Region Information Flexfield Number Segment 9',
            'Receivables Transaction Region Information Flexfield Number Segment 10',
            'Receivables Transaction Region Information Flexfield Number Segment 11',
            'Receivables Transaction Region Information Flexfield Number Segment 12',
            'Receivables Transaction Region Information Flexfield Date Segment 1',
            'Receivables Transaction Region Information Flexfield Date Segment 2',
            'Receivables Transaction Region Information Flexfield Date Segment 3',
            'Receivables Transaction Region Information Flexfield Date Segment 4',
            'Receivables Transaction Region Information Flexfield Date Segment 5',
            'Receivables Transaction Line Region Information Flexfield Number Segment 1',
            'Receivables Transaction Line Region Information Flexfield Number Segment 2',
            'Receivables Transaction Line Region Information Flexfield Number Segment 3',
            'Receivables Transaction Line Region Information Flexfield Number Segment 4',
            'Receivables Transaction Line Region Information Flexfield Number Segment 5',
            'Receivables Transaction Line Region Information Flexfield Date Segment 1',
            'Receivables Transaction Line Region Information Flexfield Date Segment 2',
            'Receivables Transaction Line Region Information Flexfield Date Segment 3',
            'Receivables Transaction Line Region Information Flexfield Date Segment 4',
            'Receivables Transaction Line Region Information Flexfield Date Segment 5',
            'Freight Charge',
            'Insurance Charge',
            'Packing Charge',
            'Miscellaneous Charge',
            'Commercial Discount',
            'Enforce Chronological Document Sequencing',
            'Payments transaction identifier',
            'Interface Status',
            'Invoice Lines Flexfield Number Segment 1',
            'Invoice Lines Flexfield Number Segment 2',
            'Invoice Lines Flexfield Number Segment 3',
            'Invoice Lines Flexfield Number Segment 4',
            'Invoice Lines Flexfield Number Segment 5',
            'Invoice Lines Flexfield Date Segment 1',
            'Invoice Lines Flexfield Date Segment 2',
            'Invoice Lines Flexfield Date Segment 3',
            'Invoice Lines Flexfield Date Segment 4',
            'Invoice Lines Flexfield Date Segment 5',
            'Invoice Transactions Flexfield Number Segment 1',
            'Invoice Transactions Flexfield Number Segment 2',
            'Invoice Transactions Flexfield Number Segment 3',
            'Invoice Transactions Flexfield Number Segment 4',
            'Invoice Transactions Flexfield Number Segment 5',
            'Invoice Transactions Flexfield Date Segment 1',
            'Invoice Transactions Flexfield Date Segment 2',
            'Invoice Transactions Flexfield Date Segment 3',
            'Invoice Transactions Flexfield Date Segment 4',
            'Invoice Transactions Flexfield Date Segment 5',
            'ADDITIONAL_LINE_CONTEXT',
            'ADDITIONAL_LINE_ATTRIBUTE1',
            'ADDITIONAL_LINE_ATTRIBUTE2',
            'ADDITIONAL_LINE_ATTRIBUTE3',
            'ADDITIONAL_LINE_ATTRIBUTE4',
            'ADDITIONAL_LINE_ATTRIBUTE5',
            'ADDITIONAL_LINE_ATTRIBUTE6',
            'ADDITIONAL_LINE_ATTRIBUTE7',
            'ADDITIONAL_LINE_ATTRIBUTE8',
            'ADDITIONAL_LINE_ATTRIBUTE9',
            'ADDITIONAL_LINE_ATTRIBUTE10',
            'ADDITIONAL_LINE_ATTRIBUTE11',
            'ADDITIONAL_LINE_ATTRIBUTE12',
            'ADDITIONAL_LINE_ATTRIBUTE13',
            'ADDITIONAL_LINE_ATTRIBUTE14',
            'ADDITIONAL_LINE_ATTRIBUTE15',
            'END'
        ]
    
    # ========================================================================
    # DATA LOADING
    # ========================================================================
    
    def load_data(self, line_items_path: str, payments_path: str, metadata_path: str, registers_path: str):
        """Load all input data files."""
        print("="*80)
        print("LOADING DATA FILES")
        print("="*80)
        
        try:
            self.line_items = pd.read_excel(line_items_path, sheet_name='Sheet1')
            self.payments = pd.read_excel(payments_path, sheet_name='Sheet1')
            self.metadata = pd.read_csv(metadata_path, encoding='utf-8-sig')
            self.registers = pd.read_csv(registers_path, encoding='utf-8-sig')
            
            self.metadata.columns = self.metadata.columns.str.strip('"').str.strip()
            
            print(f"Line Items: {len(self.line_items):,} records")
            print(f"Payments: {len(self.payments):,} records")
            print(f"Metadata: {len(self.metadata):,} records")
            print(f"Registers: {len(self.registers):,} records")
            
            self._process_line_items()
            self._process_payments()
            self._build_customer_type_cache()
            self._build_register_customer_cache()
            self._determine_invoice_customer_type()
            
        except Exception as e:
            print(f"✗ Error loading data: {str(e)}")
            raise
    
    def _process_line_items(self):
        """Process line items DataFrame."""
        self.line_items.rename(columns={
            'Order Lines/Order Ref': 'Order Ref',
            'Order Lines/Product/Barcode': 'Barcode',
            'Order Lines/Product/Name': 'Product Name',
            'Order Lines/Base Quantity': 'Quantity',
            'Order Lines/Subtotal': 'Subtotal',
            'Order Lines/Subtotal w/o Tax': 'Subtotal w/o Tax',
            'Order Lines/Order Ref/Date': 'Sale Date'
        }, inplace=True)
        
        self.line_items['Sale Date'] = pd.to_datetime(self.line_items['Sale Date'])
        self.line_items['Sale Date Only'] = self.line_items['Sale Date'].dt.date
        
        self.line_items['Store Name'] = self.line_items['Order Ref'].apply(
            lambda x: x.split('/')[0] if '/' in str(x) else str(x)
        )
        
        for _, row in self.line_items.iterrows():
            invoice = str(row['Order Ref'])
            store = row['Store Name']
            if invoice not in self.invoice_store_map:
                self.invoice_store_map[invoice] = store
        
        print(f"✓ Processed {len(self.line_items)} line items for {len(self.invoice_store_map)} invoices")
    
    def _process_payments(self):
        """Process payments DataFrame."""
        self.payments.rename(columns={
            'Order Ref': 'Order Ref',
            'Branch': 'Branch',
            'Payments/Amount': 'Amount',
            'Payments/Payment Method': 'Payment Method'
        }, inplace=True)
        
        for _, row in self.payments.iterrows():
            invoice = str(row['Order Ref'])
            if pd.isna(invoice) or invoice == 'nan':
                continue
            
            branch = str(row.get('Branch', ''))
            if pd.notna(branch) and branch != 'nan' and branch != '':
                self.invoice_register_map[invoice] = branch
        
        for _, row in self.payments.iterrows():
            invoice = str(row['Order Ref'])
            if pd.isna(invoice) or invoice == 'nan':
                continue
            
            payment_method_raw = str(row['Payment Method'])
            payment_method = self.normalize_payment_method(payment_method_raw)
            amount = float(row['Amount']) if pd.notna(row['Amount']) else 0
            
            if amount != 0:
                self.invoice_payment_map[invoice][payment_method] += amount
        
        print(f"✓ Processed payments for {len(self.invoice_payment_map)} invoices")
    
    def _build_customer_type_cache(self):
        """Build cache mapping CUSTOMER_TYPE to customer info."""
        for _, row in self.metadata.iterrows():
            customer_type = str(row.get('CUSTOMER_TYPE', '')).strip()
            subinventory = str(row.get('SUBINVENTORY', '')).strip()
            
            if customer_type and customer_type != 'nan':
                customer_info = {
                    'CUSTOMER_NAME': str(row.get('BILL_TO_NAME', '')),
                    'CUSTOMER_ACCOUNT': str(row.get('BILL_TO_ACCOUNT', '')),
                    'CUSTOMER_SITE': str(row.get('SITE_NUMBER', '')),
                    'BUSINESS_UNIT': str(row.get('BUSINESS_UNIT', 'AlQurashi-KSA')),
                    'CUSTOMER_TYPE': customer_type,
                }
                
                if customer_type not in self.customer_type_cache:
                    self.customer_type_cache[customer_type] = customer_info
            
            if subinventory and subinventory != 'nan':
                self.subinventory_to_customer_type[subinventory.upper()] = customer_type
        
        print(f"✓ Built customer type cache for {len(self.customer_type_cache)} customer types")
        for ct, info in self.customer_type_cache.items():
            print(f"    {ct}: Account={info.get('CUSTOMER_ACCOUNT')}, Site={info.get('CUSTOMER_SITE')}")
    
    def _build_register_customer_cache(self):
        """Build cache mapping REGISTER_NAME to customer info."""
        register_col = None
        for col in self.registers.columns:
            if 'REGISTER_NAME' in col.upper() or 'REGISTER' in col.upper():
                register_col = col
                break
        
        if register_col is None:
            print("⚠ No REGISTER_NAME column found")
            return
        
        for _, row in self.registers.iterrows():
            register_name = str(row.get(register_col, '')).strip()
            if pd.isna(register_name) or register_name == 'nan' or register_name == '':
                continue
            
            customer_type = 'NORMAL'
            for subinventory, ct in self.subinventory_to_customer_type.items():
                if register_name.upper() in subinventory or subinventory in register_name.upper():
                    customer_type = ct
                    break
            
            if customer_type in self.customer_type_cache:
                self.register_customer_cache[register_name.upper()] = self.customer_type_cache[customer_type].copy()
            else:
                self.register_customer_cache[register_name.upper()] = {
                    'CUSTOMER_NAME': register_name,
                    'CUSTOMER_ACCOUNT': '',
                    'CUSTOMER_SITE': '',
                    'BUSINESS_UNIT': 'AlQurashi-KSA',
                    'CUSTOMER_TYPE': 'NORMAL'
                }
        
        print(f"✓ Mapped {len(self.register_customer_cache)} registers")
    
    def _determine_invoice_customer_type(self):
        """Determine customer type for each invoice based on payment method."""
        for invoice, payment_methods in self.invoice_payment_map.items():
            customer_type = 'NORMAL'
            for payment_method in payment_methods.keys():
                payment_upper = payment_method.upper()
                if payment_upper == 'TAMARA':
                    customer_type = 'TAMARA'
                    break
                elif payment_upper == 'TABBY':
                    customer_type = 'TABBY'
                    break
            self.invoice_customer_type[invoice] = customer_type
        
        type_counts = defaultdict(int)
        for ct in self.invoice_customer_type.values():
            type_counts[ct] += 1
        print(f"✓ Determined customer types: {dict(type_counts)}")
    
    def normalize_payment_method(self, payment_method: str) -> str:
        """Normalize payment method name."""
        payment_upper = payment_method.upper().strip()
        
        if payment_upper in self.payment_method_normalization:
            return self.payment_method_normalization[payment_upper]
        
        if 'MADA' in payment_upper:
            return 'Mada'
        elif 'VISA' in payment_upper:
            return 'Visa'
        elif 'MASTERCARD' in payment_upper or 'MC' in payment_upper:
            return 'MasterCard'
        elif 'CASH' in payment_upper:
            return 'Cash'
        elif 'TAMARA' in payment_upper:
            return 'TAMARA'
        elif 'TABBY' in payment_upper:
            return 'TABBY'
        else:
            return payment_method.capitalize()
    
    def get_customer_info_by_type(self, customer_type: str) -> Dict[str, str]:
        """Get customer info by CUSTOMER_TYPE."""
        if customer_type in self.customer_type_cache:
            return self.customer_type_cache[customer_type].copy()
        return {
            'CUSTOMER_NAME': customer_type,
            'CUSTOMER_ACCOUNT': '',
            'CUSTOMER_SITE': '',
            'BUSINESS_UNIT': 'AlQurashi-KSA',
            'CUSTOMER_TYPE': customer_type
        }
    
    def is_discount_item(self, product_name) -> bool:
        """Check if item is a discount item."""
        if pd.isna(product_name):
            return True
        product_str = str(product_name).lower()
        discount_keywords = ['discount', '100.0% discount', '100% discount']
        return any(keyword in product_str for keyword in discount_keywords)
    
    def format_item_number_as_text(self, barcode) -> str:
        """
        Format item number as TEXT to prevent scientific notation in Excel.
        Adding a tab character (\t) forces Excel to treat as text.
        """
        if pd.isna(barcode):
            return ''
        
        barcode_str = str(barcode).strip()
        if '.' in barcode_str and barcode_str.endswith('.0'):
            barcode_str = barcode_str[:-2]
        
        # Add tab prefix to force Excel to treat as text
        return f"\t{barcode_str}"
    
    def calculate_unit_price(self, amount: float, quantity: float) -> float:
        """Calculate Unit Selling Price - ALWAYS POSITIVE."""
        if quantity == 0:
            return 0
        return abs(amount / quantity)
    
    # ========================================================================
    # AR INVOICE GENERATION
    # ========================================================================
    
    def format_transaction_number(self, sequence: int) -> str:
        """Format a transaction number using the configured prefix."""
        return f"{self.transaction_prefix}-{sequence:04d}"
    
    def build_transaction_number_map(self) -> Dict[str, int]:
        """
        Build a deterministic map of transaction numbers for NORMAL, TABBY, TAMARA
        starting from self.starting_sequence. Numbers only advance when a type exists.
        """
        customer_types_present = set(self.invoice_customer_type.values())
        has_normal = any(ct not in ['TAMARA', 'TABBY'] for ct in customer_types_present)
        has_tabby = 'TABBY' in customer_types_present
        has_tamara = 'TAMARA' in customer_types_present
        
        sequence = self.starting_sequence
        mapping = {}
        
        if has_normal:
            mapping['NORMAL'] = sequence
            sequence += 1
        if has_tabby:
            mapping['TABBY'] = sequence
            sequence += 1
        if has_tamara:
            mapping['TAMARA'] = sequence
            sequence += 1
        
        self.last_transaction_number = sequence - 1 if mapping else self.starting_sequence - 1
        self.transaction_number_map = mapping
        return mapping
    
    def generate_ar_invoice(self) -> pd.DataFrame:
        """Generate AR invoice records with EXACT column headers."""
        print("\n" + "="*80)
        print("GENERATING AR INVOICE RECORDS")
        print("="*80)
        
        all_records = []
        discount_count = 0
        regular_count = 0
        invoice_count = 0
        tabby_tamara_count = 0
        normal_count = 0
        total_sales_amount = 0.0
        
        self.ar_segment_counter = 1
        self.invoice_to_ar_transaction = {}
        self.build_transaction_number_map()
        
        unique_invoices = self.line_items['Order Ref'].unique()
        
        print("\n" + "-"*80)
        print("PROCESSING INVOICES")
        print("-"*80)
        
        for invoice_number in unique_invoices:
            invoice_number = str(invoice_number)
            invoice_count += 1
            
            invoice_items = self.line_items[self.line_items['Order Ref'].astype(str) == invoice_number]
            
            if invoice_items.empty:
                continue
            
            first_item = invoice_items.iloc[0]
            sale_date = first_item['Sale Date Only']
            store_name = first_item['Store Name']
            
            register_name = self.get_register_name_for_invoice(invoice_number)
            customer_type = self.invoice_customer_type.get(invoice_number, 'NORMAL')
            customer_info = self.get_customer_info_by_type(customer_type)
            
            if customer_type in ['TAMARA', 'TABBY']:
                tabby_tamara_count += 1
            else:
                normal_count += 1
            
            customer_key = 'TABBY' if customer_type == 'TABBY' else 'TAMARA' if customer_type == 'TAMARA' else 'NORMAL'
            transaction_number = self.format_transaction_number(
                self.transaction_number_map.get(customer_key, self.starting_sequence)
            )
            self.invoice_to_ar_transaction[invoice_number] = transaction_number
            
            print(f"\nInvoice: {invoice_number}")
            print(f"  Transaction Number: {transaction_number}")
            print(f"  Customer Type: {customer_type}")
            print(f"  Customer Account: {customer_info.get('CUSTOMER_ACCOUNT', 'N/A')}")
            print(f"  Customer Site: {customer_info.get('CUSTOMER_SITE', 'N/A')}")
            print(f"  Date: {sale_date}, Items: {len(invoice_items)}")
            
            invoice_total = 0
            
            for _, item in invoice_items.iterrows():
                product_name = str(item.get('Product Name', ''))
                barcode = item.get('Barcode', '')
                quantity = float(item.get('Quantity', 0))
                subtotal_without_tax = float(item.get('Subtotal w/o Tax', 0))
                
                is_discount = self.is_discount_item(product_name)
                
                if is_discount:
                    inventory_item_number = ''
                    line_description = "Discount Item"
                    memo_text = "Discount Item"
                    transaction_amount = subtotal_without_tax
                    unit_price = 0
                    discount_count += 1
                else:
                    inventory_item_number = self.format_item_number_as_text(barcode)
                    line_description = product_name[:240] if product_name != 'nan' else ''
                    memo_text = ''
                    transaction_amount = subtotal_without_tax
                    unit_price = self.calculate_unit_price(transaction_amount, quantity)
                    regular_count += 1
                
                invoice_total += transaction_amount
                date_str_full = sale_date.strftime('%Y-%m-%d 00:00:00')
                conversion_date_str = sale_date.strftime('%Y-%m-%d')
                
                # Create record with ALL columns - using dictionary with default empty strings
                record = {col: '' for col in self.ar_columns}
                
                # Populate only the fields we have data for
                record['Transaction Batch Source Name'] = self.ar_static_fields['Transaction Batch Source Name']
                record['Transaction Type Name'] = self.ar_static_fields['Transaction Type Name']
                record['Payment Terms'] = self.ar_static_fields['Payment Terms']
                record['Transaction Date'] = date_str_full
                record['Accounting Date'] = date_str_full
                record['Transaction Number'] = transaction_number
                record['Bill-to Customer Account Number'] = customer_info.get('CUSTOMER_ACCOUNT', '')
                record['Bill-to Customer Site Number'] = customer_info.get('CUSTOMER_SITE', '')
                record['Transaction Line Type'] = self.ar_static_fields['Transaction Line Type']
                record['Transaction Line Description'] = line_description
                record['Currency Code'] = self.ar_static_fields['Currency Code']
                record['Currency Conversion Type'] = self.ar_static_fields['Currency Conversion Type']
                record['Currency Conversion Date'] = conversion_date_str
                record['Currency Conversion Rate'] = self.ar_static_fields['Currency Conversion Rate']
                record['Transaction Line Amount'] = round(transaction_amount, 2)
                record['Transaction Line Quantity'] = quantity
                record['Unit Selling Price'] = round(unit_price, 2)
                record['Line Transactions Flexfield Context'] = self.ar_static_fields['Line Transactions Flexfield Context']
                record['Line Transactions Flexfield Segment 1'] = f"LEGACY{self.ar_segment_counter:08d}"
                record['Line Transactions Flexfield Segment 2'] = f"LEGACY{self.ar_segment_counter:08d}"
                record['Tax Classification Code'] = 'OUTPUT-GOODS-DOM-15%'
                record['Sales Order Number'] = invoice_number
                record['Unit of Measure Code'] = self.ar_static_fields['Unit of Measure Code']
                record['Default Taxation Country'] = self.ar_static_fields['Default Taxation Country']
                record['Inventory Item Number'] = inventory_item_number
                record['Comments'] = self.ar_static_fields['Comments']
                record['END'] = self.ar_static_fields['END']
                
                if is_discount and memo_text:
                    record['Memo Line Name'] = memo_text
                
                self.ar_segment_counter += 1
                all_records.append(record)
            
            print(f"  Invoice Total: {invoice_total:.2f} SAR")
            total_sales_amount += invoice_total
        
        print(f"\n{'='*80}")
        print("GENERATION COMPLETE")
        print(f"  Total Invoices: {invoice_count}")
        if 'NORMAL' in self.transaction_number_map:
            print(f"  NORMAL (Cash/Card/Visa/Mada/Amex/MC): {normal_count} (Transaction: {self.format_transaction_number(self.transaction_number_map['NORMAL'])})")
        if 'TABBY' in self.transaction_number_map:
            print(f"  TABBY: {tabby_tamara_count} (Transaction: {self.format_transaction_number(self.transaction_number_map['TABBY'])})")
        if 'TAMARA' in self.transaction_number_map:
            print(f"  TAMARA: {tabby_tamara_count} (Transaction: {self.format_transaction_number(self.transaction_number_map['TAMARA'])})")
        print(f"  Total Line Items: {len(all_records):,}")
        print(f"  Regular items: {regular_count}, Discount items: {discount_count}")
        print(f"  Total Columns: {len(self.ar_columns)}")
        print(f"  Transaction number map: {self.transaction_number_map}")
        print(f"  Last transaction number used: {self.last_transaction_number}")
        
        self.generation_stats = {
            'invoice_count': invoice_count,
            'line_item_count': len(all_records),
            'total_sales_amount': round(total_sales_amount, 2),
            'transaction_number_map': self.transaction_number_map,
            'last_transaction_number': self.last_transaction_number,
            'transaction_prefix': self.transaction_prefix
        }
        
        return pd.DataFrame(all_records)
    
    def get_register_name_for_invoice(self, invoice_number: str) -> str:
        """Get register name for an invoice."""
        if str(invoice_number) in self.invoice_register_map:
            return self.invoice_register_map[str(invoice_number)]
        return self.invoice_store_map.get(str(invoice_number), 'UNKNOWN')
    
    # ========================================================================
    # RECEIPT GENERATION
    # ========================================================================
    
    def get_invoice_sale_date(self, invoice_number: str):
        """Get sale date for an invoice."""
        if self.line_items is None:
            return datetime.now()
        invoice_items = self.line_items[self.line_items['Order Ref'].astype(str) == str(invoice_number)]
        if not invoice_items.empty:
            return invoice_items.iloc[0]['Sale Date']
        return datetime.now()
    
    def aggregate_payments(self) -> Dict[tuple, Dict]:
        """Aggregate payments by (Register Name, Date, Payment Method)."""
        print("\n" + "="*80)
        print("AGGREGATING PAYMENTS FOR RECEIPTS")
        print(f"  Including: {sorted(self.receipt_payment_methods)}")
        print("="*80)
        
        aggregator = defaultdict(lambda: {
            'amount': 0,
            'invoices': [],
            'customer_account': None,
            'customer_site': None,
            'customer_name': None,
            'business_unit': None,
            'ar_transaction_numbers': set(),
            'register_name': None
        })
        
        for invoice_number, payment_methods in self.invoice_payment_map.items():
            customer_type = self.invoice_customer_type.get(invoice_number, 'NORMAL')
            if customer_type not in ['NORMAL']:
                continue
            
            register_name = self.get_register_name_for_invoice(invoice_number)
            sale_date = self.get_invoice_sale_date(str(invoice_number))
            date_str = sale_date.strftime('%Y-%m-%d')
            
            ar_transaction_number = self.invoice_to_ar_transaction.get(str(invoice_number), '')
            customer_info = self.get_customer_info_by_type('NORMAL')
            
            for payment_method, amount in payment_methods.items():
                if payment_method not in self.receipt_payment_methods:
                    continue
                if amount == 0:
                    continue
                
                key = (register_name, date_str, payment_method)
                
                aggregator[key]['amount'] += amount
                aggregator[key]['invoices'].append({
                    'invoice_number': invoice_number,
                    'ar_transaction_number': ar_transaction_number,
                    'amount': amount
                })
                aggregator[key]['ar_transaction_numbers'].add(ar_transaction_number)
                aggregator[key]['customer_account'] = customer_info.get('CUSTOMER_ACCOUNT', '')
                aggregator[key]['customer_site'] = customer_info.get('CUSTOMER_SITE', '')
                aggregator[key]['customer_name'] = customer_info.get('CUSTOMER_NAME', register_name)
                aggregator[key]['business_unit'] = customer_info.get('BUSINESS_UNIT', 'AlQurashi-KSA')
                aggregator[key]['register_name'] = register_name
                aggregator[key]['sale_date'] = sale_date
        
        print(f"✓ Aggregated into {len(aggregator)} receipt groups")
        return aggregator
    
    def generate_receipts(self) -> Dict[str, pd.DataFrame]:
        """Generate receipt records."""
        print("\n" + "="*80)
        print("GENERATING RECEIPT RECORDS")
        print(f"  Only for: {sorted(self.receipt_payment_methods)}")
        print("="*80)
        
        payment_aggregator = self.aggregate_payments()
        
        if not payment_aggregator:
            print("⚠ No receipt groups found")
            return {}
        
        register_date_groups = defaultdict(list)
        for (register, date, method), data in payment_aggregator.items():
            register_date_groups[(register, date)].append((method, data))
        
        print(f"\n✓ Creating {len(register_date_groups)} receipt files")
        
        receipt_files = {}
        seq_counter = 1
        
        for (register_name, date_str), methods_data in register_date_groups.items():
            print(f"\n📁 Creating: {register_name} - {date_str}")
            
            records = []
            seq = 1
            
            try:
                sale_date = datetime.strptime(date_str, '%Y-%m-%d')
                batch_date = sale_date.strftime('%Y-%m-%d 00:00:00')
            except:
                batch_date = datetime.now().strftime('%Y-%m-%d 00:00:00')
            
            business_unit = None
            customer_name = None
            customer_account = None
            customer_site = None
            
            for payment_method, data in methods_data:
                if business_unit is None:
                    business_unit = data.get('business_unit', 'AlQurashi-KSA')
                    customer_name = data.get('customer_name', register_name)
                    customer_account = data.get('customer_account', '')
                    customer_site = data.get('customer_site', '')
                
                bank_info = self.payment_method_bank_mapping.get(payment_method, self.default_bank)
                
                first_invoice = data['invoices'][0] if data['invoices'] else {}
                ar_number = first_invoice.get('ar_transaction_number', '')
                
                receipt_number = f"{payment_method}-{ar_number}" if ar_number else f"RCPT-{seq_counter:06d}"
                batch_name = f"Receipt_Import_{register_name}_{date_str.replace('-', '')}"
                
                record = {
                    'Business Unit': business_unit,
                    'Batch Source': 'Spreadsheet',
                    'Batch Name': batch_name,
                    'Receipt Method': payment_method,
                    'Remittance Bank': bank_info[0],
                    'Remittance Bank Account': bank_info[1],
                    'Batch Date': batch_date,
                    'Accounting Date': batch_date,
                    'Deposit Date': batch_date,
                    'Currency': 'SAR',
                    'Sequence Number': f"{seq:04d}",
                    'Receipt Number': receipt_number,
                    'Receipt Amount': round(data['amount'], 2),
                    'Receipt Date': batch_date,
                    'Accounting Date_2': batch_date,
                    'Currency_2': 'SAR',
                    'Document Number': '',
                    'Customer Name': customer_name,
                    'Customer Account Number': customer_account,
                    'Customer Site Number': customer_site
                }
                
                records.append(record)
                print(f"    Receipt: {receipt_number} - {payment_method}: {data['amount']:,.2f} SAR")
                seq += 1
                seq_counter += 1
            
            safe_name = re.sub(r'[^a-zA-Z0-9]', '_', register_name)
            filename = f"Receipt_Import_{safe_name}_{date_str.replace('-', '')}.csv"
            receipt_files[filename] = pd.DataFrame(records)
        
        total_receipts = sum(len(df) for df in receipt_files.values())
        print(f"\n✓ TOTAL: {total_receipts} receipts across {len(receipt_files)} files")
        
        return receipt_files
    
    # ========================================================================
    # OUTPUT SAVING
    # ========================================================================
    
    def save_ar_output(self, df: pd.DataFrame):
        """Save AR invoice output."""
        if df.empty:
            print("⚠ No AR invoice data to save")
            return
        
        folder = os.path.join(self.base_output_dir, 'AR_Invoices')
        Path(folder).mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(folder, f'AR_Invoice_Import_{timestamp}.csv')
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"\n✓ Saved {len(df):,} AR records to {output_file}")
        print(f"✓ Total columns: {len(df.columns)}")
        
        summary_file = os.path.join(folder, f'AR_INVOICE_SUMMARY_{timestamp}.txt')
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("AR INVOICE GENERATION SUMMARY\n")
            f.write("="*80 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Line Items: {len(df):,}\n")
            f.write(f"Total Amount: {df['Transaction Line Amount'].sum():,.2f} SAR\n")
            f.write(f"Total Columns: {len(df.columns)}\n\n")
            f.write("TRANSACTION NUMBER LOGIC:\n")
            f.write("  - Cash/Mada/Visa: BLK-XXXX-0000001 (same number, no increment)\n")
            f.write("  - TAMARA/TABBY: BLK-XXXX-0000001, BLK-XXXX-0000002 (increments)\n\n")
            f.write("ITEM NUMBER FORMAT: Tab prefix to prevent scientific notation\n")
        
        print(f"✓ Summary: {summary_file}")
    
    def save_receipt_output(self, receipt_files: Dict[str, pd.DataFrame]):
        """Save receipt output."""
        if not receipt_files:
            print("⚠ No receipt data to save")
            return
        
        folder = os.path.join(self.base_output_dir, 'Receipts')
        Path(folder).mkdir(parents=True, exist_ok=True)
        
        for filename, df in receipt_files.items():
            output_file = os.path.join(folder, filename)
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"✓ Saved {len(df)} receipts to {filename}")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        summary_file = os.path.join(folder, f'RECEIPT_SUMMARY_{timestamp}.txt')
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("RECEIPT GENERATION SUMMARY\n")
            f.write("="*80 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Files: {len(receipt_files)}\n")
            f.write(f"Total Receipts: {sum(len(df) for df in receipt_files.values()):,}\n\n")
            f.write("RECEIPT CREATED ONLY FOR: Cash, Mada, Visa\n")
            f.write("RECEIPT NOT CREATED FOR: Tabby, Tamara\n")
        
        print(f"✓ Summary: {summary_file}")
    
    def create_mapping_guide(self):
        """Create mapping guides."""
        mapping_file = os.path.join(self.base_output_dir, 'PAYMENT_METHOD_MAPPING_GUIDE.txt')
        with open(mapping_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("CUSTOMER MAPPING\n")
            f.write("="*80 + "\n\n")
            for ct, info in sorted(self.customer_type_cache.items()):
                f.write(f"{ct}: Account={info.get('CUSTOMER_ACCOUNT')}, Site={info.get('CUSTOMER_SITE')}\n")
        
        print(f"✓ Mapping guide: {mapping_file}")
    
    # ========================================================================
    # MAIN PIPELINE
    # ========================================================================
    
    def run(self, line_items_path: str, payments_path: str, metadata_path: str, registers_path: str):
        """Run the complete integration pipeline."""
        print("\n" + "="*80)
        print("ORACLE FUSION INTEGRATION PIPELINE")
        print("="*80)
        
        self.load_data(line_items_path, payments_path, metadata_path, registers_path)
        
        ar_df = self.generate_ar_invoice()
        self.save_ar_output(ar_df)
        
        receipt_files = self.generate_receipts()
        self.save_receipt_output(receipt_files)
        
        self.create_mapping_guide()
        
        print("\n" + "="*80)
        print("✅ INTEGRATION COMPLETE")
        print("="*80)
        print(f"\nOutput: {self.base_output_dir}/")
        print(f"  - AR_Invoices/AR_Invoice_Import_*.csv")
        print(f"  - Receipts/Receipt_Import_*.csv")
        print(f"\nAR Invoice has {len(self.ar_columns)} columns (exact match to template)")
        print(f"Last transaction number used: {self.last_transaction_number}")
        print("="*80)
        
        return {
            'ar_df': ar_df,
            'receipt_files': receipt_files,
            'ar_stats': self.generation_stats
        }


# ========================================================================
# MAIN EXECUTION
# ========================================================================

def main():
    """Main entry point."""
    
    input_files = {
        'line_items': 'Point of Sale Orders (pos.order) - 2026-04-12T162030.266.xlsx',
        'payments': 'Point of Sale Orders (pos.order) - 2026-04-12T162041.258.xlsx',
        'metadata': 'FUSION_SALES_METADATA_202604121703.csv',
        'registers': 'VENDHQ_REGISTERS_202604121654.csv'
    }
    
    integration = OracleFusionIntegration(base_output_dir="ORACLE_FUSION_OUTPUT")
    
    try:
        integration.run(
            input_files['line_items'],
            input_files['payments'],
            input_files['metadata'],
            input_files['registers']
        )
    except FileNotFoundError as e:
        print(f"\n❌ File not found: {str(e)}")
        print("\nRequired files:")
        for name, path in input_files.items():
            print(f"  - {path}")
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
