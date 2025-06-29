import pandas as pd
import os
import re
from pathlib import Path

def merge_csv_files(input_folder='crawled_results', output_file='merged_results.csv'):
    """
    Merge tất cả các file CSV trong folder thành một file CSV duy nhất
    
    Args:
        input_folder (str): Đường dẫn đến folder chứa các file CSV
        output_file (str): Tên file CSV output
    
    Returns:
        bool: True nếu merge thành công, False nếu có lỗi
    """
    try:
        # Kiểm tra folder có tồn tại không
        folder_path = Path(input_folder)
        if not folder_path.exists():
            print(f"Folder {input_folder} không tồn tại!")
            return False
        
        # Tìm tất cả file CSV trong folder
        csv_files = list(folder_path.glob('*.csv'))
        
        if not csv_files:
            print(f"Không tìm thấy file CSV nào trong folder {input_folder}")
            return False
        
        # Sort theo số batch tăng dần
        def extract_batch_number(file_path):
            """Trích xuất số batch từ tên file"""
            filename = file_path.stem  # Lấy tên file không có extension
            # Tìm số cuối cùng trong tên file (số batch)
            match = re.search(r'(\d+)$', filename)
            if match:
                return int(match.group(1))
            return 0  # Nếu không tìm thấy số, trả về 0
        
        csv_files.sort(key=extract_batch_number)
        
        print(f"Tìm thấy {len(csv_files)} file CSV (đã sort theo batch number):")
        for file in csv_files:
            batch_num = extract_batch_number(file)
            print(f"  - {file.name} (batch: {batch_num})")
        
        # Đọc và merge tất cả file CSV
        dataframes = []
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                # Thêm cột source_file để biết data từ file nào
                df['source_file'] = csv_file.name
                # Thêm cột batch_number để biết thứ tự batch
                df['batch_number'] = extract_batch_number(csv_file)
                dataframes.append(df)
                print(f"Đã đọc {len(df)} dòng từ {csv_file.name}")
            except Exception as e:
                print(f"Lỗi khi đọc file {csv_file.name}: {e}")
        
        if not dataframes:
            print("Không có file CSV nào được đọc thành công!")
            return False
        
        # Merge tất cả dataframes
        merged_df = pd.concat(dataframes, ignore_index=True)
        
        # Lưu file merged
        merged_df.to_csv(output_file, index=False, encoding='utf-8')
        
        print(f"\nĐã merge thành công!")
        print(f"Tổng số dòng: {len(merged_df)}")
        print(f"File output: {output_file}")
        
        return True
        
    except Exception as e:
        print(f"Lỗi khi merge file: {e}")
        return False

# Sử dụng hàm
if __name__ == "__main__":
    merge_csv_files(input_folder='crawled_results_mien_bac', output_file='merged_results_mien_bac.csv')