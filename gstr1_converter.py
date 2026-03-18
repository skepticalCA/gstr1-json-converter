import os
import json
import glob
import time
import pandas as pd
from collections import defaultdict

def extract_b2b(data, base_meta):
    rows = []
    for ctin_obj in data.get('b2b', []):
        ctin = ctin_obj.get('ctin', '')
        for inv in ctin_obj.get('inv', []):
            inv_meta = {
                'ctin': ctin,
                'inum': inv.get('inum', ''),
                'idt': inv.get('idt', ''),
                'val': inv.get('val', ''),
                'pos': inv.get('pos', ''),
                'rchrg': inv.get('rchrg', ''),
                'inv_typ': inv.get('inv_typ', ''),
                'irn': inv.get('irn', ''),
                'irngendate': inv.get('irngendate', '')
            }
            for itm in inv.get('itms', []):
                itm_det = itm.get('itm_det', {})
                row = {**base_meta, **inv_meta, 'num': itm.get('num', '')}
                row.update(itm_det)
                rows.append(row)
    return rows

def extract_b2cl(data, base_meta):
    rows = []
    for pos_obj in data.get('b2cl', []):
        pos = pos_obj.get('pos', '')
        for inv in pos_obj.get('inv', []):
            inv_meta = {
                'pos': pos,
                'inum': inv.get('inum', ''),
                'idt': inv.get('idt', ''),
                'val': inv.get('val', '')
            }
            for itm in inv.get('itms', []):
                itm_det = itm.get('itm_det', {})
                row = {**base_meta, **inv_meta, 'num': itm.get('num', '')}
                row.update(itm_det)
                rows.append(row)
    return rows

def extract_b2cs(data, base_meta):
    rows = []
    for itm in data.get('b2cs', []):
        row = {**base_meta}
        row.update(itm)
        rows.append(row)
    return rows

def extract_exp(data, base_meta):
    rows = []
    for exp_obj in data.get('exp', []):
        exp_typ = exp_obj.get('exp_typ', '')
        for inv in exp_obj.get('inv', []):
            inv_meta = {
                'exp_typ': exp_typ,
                'inum': inv.get('inum', ''),
                'idt': inv.get('idt', ''),
                'val': inv.get('val', ''),
                'sbpcode': inv.get('sbpcode', ''),
                'sbnum': inv.get('sbnum', ''),
                'sbdt': inv.get('sbdt', '')
            }
            for itm in inv.get('itms', []):
                row = {**base_meta, **inv_meta}
                row.update(itm)
                rows.append(row)
    return rows

def extract_cdnr(data, base_meta):
    rows = []
    for ctin_obj in data.get('cdnr', []):
        ctin = ctin_obj.get('ctin', '')
        for nt in ctin_obj.get('nt', []):
            nt_meta = {
                'ctin': ctin,
                'nt_num': nt.get('nt_num', ''),
                'nt_dt': nt.get('nt_dt', ''),
                'ntty': nt.get('ntty', ''),
                'inum': nt.get('inum', ''),
                'idt': nt.get('idt', ''),
                'val': nt.get('val', '')
            }
            for itm in nt.get('itms', []):
                itm_det = itm.get('itm_det', {})
                row = {**base_meta, **nt_meta, 'num': itm.get('num', '')}
                row.update(itm_det)
                rows.append(row)
    return rows

def extract_cdnur(data, base_meta):
    rows = []
    for nt in data.get('cdnur', []):
        nt_meta = {
            'typ': nt.get('typ', ''),
            'nt_num': nt.get('nt_num', ''),
            'nt_dt': nt.get('nt_dt', ''),
            'ntty': nt.get('ntty', ''),
            'inum': nt.get('inum', ''),
            'idt': nt.get('idt', ''),
            'val': nt.get('val', '')
        }
        for itm in nt.get('itms', []):
            itm_det = itm.get('itm_det', {})
            row = {**base_meta, **nt_meta, 'num': itm.get('num', '')}
            row.update(itm_det)
            rows.append(row)
    return rows

def extract_hsn(data, base_meta):
    rows = []
    hsn_data = data.get('hsn', {})
    if isinstance(hsn_data, dict):
        # Handle formats having data directly, or split into hsn_b2c, hsn_b2b
        for key in ['data', 'hsn_b2b', 'hsn_b2c']:
            items = hsn_data.get(key, [])
            for itm in items:
                row = {**base_meta, 'hsn_type': key}
                row.update(itm)
                rows.append(row)
    elif isinstance(hsn_data, list):
        for itm in hsn_data:
            row = {**base_meta}
            row.update(itm)
            rows.append(row)
    return rows

def extract_doc_issue(data, base_meta):
    rows = []
    doc_issue = data.get('doc_issue', {})
    for doc_det in doc_issue.get('doc_det', []):
        doc_num = doc_det.get('doc_num', '')
        for doc in doc_det.get('docs', []):
            row = {**base_meta, 'doc_num': doc_num}
            row.update(doc)
            rows.append(row)
    return rows

def extract_flat_list(data, key, base_meta):
    rows = []
    for itm in data.get(key, []):
        row = {**base_meta}
        if isinstance(itm, dict):
            row.update(itm)
        else:
            row['value'] = itm
        rows.append(row)
    return rows


def main():
    folder_path = '/Users/arvind/Downloads/JSON FILES/all_json'
    output_file = '/Users/arvind/Downloads/JSON FILES/GSTR1_Consolidated.xlsx'
    
    file_list = glob.glob(os.path.join(folder_path, '*.json'))
    if not file_list:
        print(f"No JSON files found in {folder_path}.")
        return

    print(f"Found {len(file_list)} JSON files. Starting processing...")
    
    # Dictionary to hold lists of rows for each section
    all_data = defaultdict(list)
    
    start_time = time.time()
    
    for i, file_path in enumerate(file_list, 1):
        filename = os.path.basename(file_path)
        
        if i % 10 == 0 or i == len(file_list):
            print(f"Processing file {i}/{len(file_list)}: {filename}")
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"  [ERROR] Cannot read or parse {filename}: {e}. Skipping.")
            continue
            
        # Top-level meta
        gstin = data.get('gstin', '')
        fp = data.get('fp', '')
        
        base_meta = {
            'Source_File': filename,
            'GSTIN': gstin,
            'Filing_Period': fp
        }
        
        # Extract sections
        if 'b2b' in data:
            all_data['B2B'].extend(extract_b2b(data, base_meta))
        if 'b2cl' in data:
            all_data['B2CL'].extend(extract_b2cl(data, base_meta))
        if 'b2cs' in data:
            all_data['B2CS'].extend(extract_b2cs(data, base_meta))
        if 'exp' in data:
            all_data['EXP'].extend(extract_exp(data, base_meta))
        if 'cdnr' in data:
            all_data['CDNR'].extend(extract_cdnr(data, base_meta))
        if 'cdnur' in data:
            all_data['CDNUR'].extend(extract_cdnur(data, base_meta))
        if 'hsn' in data:
            all_data['HSN'].extend(extract_hsn(data, base_meta))
        if 'doc_issue' in data:
            all_data['DOC_ISSUE'].extend(extract_doc_issue(data, base_meta))
            
        # Treat these as flat lists
        for key in ['nil', 'txpd', 'at', 'exemp']:
            if key in data:
                all_data[key.upper()].extend(extract_flat_list(data, key, base_meta))

    print(f"\nFinished parsing {len(file_list)} files in {time.time() - start_time:.2f} seconds.")
    print("Converting sections to DataFrames...")
    
    # Convert to DataFrames
    dfs = {}
    summary_data = []
    
    for section, rows in all_data.items():
        if rows:
            df = pd.DataFrame(rows)
            dfs[section] = df
            summary_data.append({
                'Section': section,
                'Row_Count': len(df)
            })
            
    # Add summary DataFrame
    if summary_data:
        summary_df = pd.DataFrame(summary_data)
        summary_df.loc['Total'] = summary_df.sum(numeric_only=True)
        summary_df.at['Total', 'Section'] = 'TOTAL'
        dfs['Summary'] = summary_df
    
    print(f"Writing to Excel file: {output_file}...")
    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Write summary first
            if 'Summary' in dfs:
                dfs['Summary'].to_excel(writer, sheet_name='Summary', index=False)
            
            # Write other sections
            for section, df in dfs.items():
                if section != 'Summary':
                    # Truncate sheet names to 31 chars as per Excel limit
                    sheet_name = section[:31]
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    
        print("\nSUCCESS! Excel file generated successfully.")
    except Exception as e:
        print(f"\n[ERROR] Failed to write Excel file: {e}")
        print("Please ensure you have openpyxl installed (pip install openpyxl).")

if __name__ == "__main__":
    main()
