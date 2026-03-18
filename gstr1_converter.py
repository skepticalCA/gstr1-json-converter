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


def process_json_data_to_excel(json_files_data, output_target):
    """
    json_files_data: list of tuples -> (filename, parsed_dict)
    output_target: file path string OR BytesIO buffer
    """
    all_data = defaultdict(list)
    start_time = time.time()
    
    for filename, data in json_files_data:
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

    print(f"\nFinished parsing {len(json_files_data)} files in {time.time() - start_time:.2f} seconds.")
    
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
            
    if summary_data:
        summary_df = pd.DataFrame(summary_data)
        summary_df.loc['Total'] = summary_df.sum(numeric_only=True)
        summary_df.at['Total', 'Section'] = 'TOTAL'
        dfs['Summary'] = summary_df
    
    with pd.ExcelWriter(output_target, engine='openpyxl') as writer:
        if 'Summary' in dfs:
            dfs['Summary'].to_excel(writer, sheet_name='Summary', index=False)
        for section, df in dfs.items():
            if section != 'Summary':
                sheet_name = section[:31]
                df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    return dfs

def main():
    folder_path = '/Users/arvind/Downloads/JSON FILES/all_json'
    output_file = 'GSTR1_Consolidated.xlsx'
    
    file_list = glob.glob(os.path.join(folder_path, '*.json'))
    if not file_list:
        print("No JSON files found in all_json directory.")
        return

    json_data = []
    for file_path in file_list:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                json_data.append((os.path.basename(file_path), json.load(f)))
        except Exception as e:
            print(f"Skipping {file_path}: {e}")

    process_json_data_to_excel(json_data, output_file)
    print("SUCCESS! Excel file generated locally.")

if __name__ == "__main__":
    main()
