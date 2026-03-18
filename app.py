import streamlit as st
import json
from io import BytesIO
from gstr1_converter import process_json_data_to_excel

st.set_page_config(page_title="GSTR-1 JSON to Excel Converter", page_icon="📄", layout="centered")

st.title("📄 GSTR-1 JSON to Excel Converter")
st.markdown("Upload your downloaded GSTR-1 `.json` files below. The tool will parse deeply nested data (B2B, B2CS, HSN, etc.) and generate a clean, consolidated Excel file for you.")

uploaded_files = st.file_uploader(
    "Upload GSTR-1 JSON Files", 
    type=['json'], 
    accept_multiple_files=True
)

if uploaded_files:
    st.info(f"{len(uploaded_files)} file(s) uploaded. Ready to process.")
    
    if st.button("Generate Excel File", type="primary"):
        with st.spinner("Parsing JSON files and flattening data... Please wait."):
            try:
                # 1. Read files into memory
                json_data_list = []
                for idx, file in enumerate(uploaded_files):
                    # decode json to python dictionary
                    content = file.read().decode("utf-8")
                    data = json.loads(content)
                    json_data_list.append((file.name, data))
                
                # 2. Process data to an in-memory BytesIO explicitly for Excel
                output_buffer = BytesIO()
                process_json_data_to_excel(json_data_list, output_buffer)
                
                # 3. Serve the result
                output_buffer.seek(0)
                
                st.success(f"✅ Processing complete! Converted {len(uploaded_files)} files into Excel sheets.")
                
                st.download_button(
                    label="⬇️ Download Consolidated Excel",
                    data=output_buffer,
                    file_name="GSTR1_Consolidated.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            except Exception as e:
                st.error(f"An error occurred during processing. Details: {str(e)}")
else:
    st.write("Please upload one or more GSTR-1 JSON files to get started.")
