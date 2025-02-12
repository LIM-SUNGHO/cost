
# 1단계_BOM ------------------------------------------------------------------------------------------------------------------------------------------
import pandas as pd
import os

# 현재 스크립트의 디렉토리를 작업 디렉토리로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)

UPLOAD_DIR = "uploads"

# 파일 로드
file_path = os.path.join(UPLOAD_DIR, '2.생산사업장별생산품목등록.csv')
df = pd.read_csv(file_path)

# '생산종료일'을 날짜로 처리 가능하도록 변환 (예외적인 날짜도 처리)
def custom_date_parser(date_str):
    if date_str in ['9999-12-31', '2999-12-31', '2122-01-01']:
        return pd.Timestamp(date_str)
    try:
        return pd.to_datetime(date_str, errors='coerce')
    except ValueError:
        return pd.NaT

df['생산종료일_parsed'] = df['생산종료일'].apply(custom_date_parser)

# '품번' 별로 최종 차수 확인 및 '이전차수' 표시 로직
def determine_production_status(row, grouped_df):
    if row['생산종료일_parsed'] == grouped_df.get_group(row['품번'])['생산종료일_parsed'].max():
        return "최종차수"
    else:
        return "이전차수"

grouped = df.groupby('품번')
df['최종차수'] = df.apply(determine_production_status, grouped_df=grouped, axis=1)

# 최종 결과를 새로운 CSV 파일로 저장 (2.생산사업장별생산품목등록.csv)
new_file_path = os.path.join(UPLOAD_DIR, '2.생산사업장별생산품목등록.csv')
df.to_csv(new_file_path, index=False, encoding='utf-8-sig')

print("최종차수 및 이전차수 정보가 손익 파일에 저장되었습니다.")

# 2단계_BOM ------------------------------------------------------------------------------------------------------------------------------------------

import pandas as pd
import os

# 현재 스크립트의 디렉토리를 작업 디렉토리로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)

# 파일 로드
file_path = os.path.join(UPLOAD_DIR, '2.생산사업장별생산품목등록.csv')
df = pd.read_csv(file_path)

# '최종수정일' 날짜/시간 변환 함수 정의
def parse_datetime(dt_str):
    try:
        parts = dt_str.split(' ')
        date_part = parts[0]
        am_pm = parts[1]
        time_part = parts[2]

        hour, minute, second = map(int, time_part.split(':'))

        if am_pm == '오후' and hour != 12:
            hour += 12
        elif am_pm == '오전' and hour == 12:
            hour = 0

        time_part = f"{hour:02d}:{minute:02d}:{second:02d}"
        return pd.to_datetime(f"{date_part} {time_part}", format='%Y-%m-%d %H:%M:%S', errors='coerce')
    except (ValueError, IndexError):
        return pd.NaT

# '최종수정일' 열의 날짜/시간 변환 적용
df['최종수정일_parsed'] = df['최종수정일'].apply(parse_datetime)

# '최종차수'와 '최종수정일'을 기준으로 최종 차수 결정
def determine_final_status(row, latest_final_dates):
    if row['최종수정일_parsed'] == latest_final_dates.get(row['품번'], pd.NaT) and row['최종차수'] == "최종차수":
        return "최종차수"
    return "이전차수"

# '최종차수'가 '최종차수'인 경우의 '최종수정일_parsed' 최대값 계산
latest_final_dates = df[df['최종차수'] == "최종차수"].groupby('품번')['최종수정일_parsed'].max().to_dict()

# 수정된 로직 적용하여 '최종차수지정' 열 생성
df['최종차수지정'] = df.apply(determine_final_status, axis=1, latest_final_dates=latest_final_dates)

# 결과 파일 이름 설정
# 최종 결과를 새 CSV 파일로 저장
df.to_csv(file_path, index=False, encoding='utf-8-sig')

print("최종차수지정 정보가 업데이트되었습니다.")

# 3단계_BOM ------------------------------------------------------------------------------------------------------------------------------------------

import pandas as pd
import os

# 현재 스크립트의 디렉토리를 작업 디렉토리로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)

# CSV 파일 불러오기
file_path = os.path.join(UPLOAD_DIR,'제품별공정별소요자재조회.csv')
file_path2 = os.path.join(UPLOAD_DIR,'2.생산사업장별생산품목등록.csv')

# dtype을 사용하여 '자재번호'를 문자열로 처리
df_process = pd.read_csv(file_path, low_memory=False)
df_bom = pd.read_csv(file_path2, low_memory=False)

# 최종차수 데이터만 추출
df_final_bom = df_bom[df_bom['최종차수지정'] == '최종차수']
final_bom_keys = df_final_bom[['품번', 'BOM차수']].values

# 최종차수 기준의 '제품별공정별소요자재조회' 데이터 추출
def is_final_bom(row):
    return any((row['품번'] == key[0] and row['BOM차수'] == key[1]) for key in final_bom_keys)

df_final_process = df_process[df_process.apply(is_final_bom, axis=1)]

# 결과 파일 이름 설정
new_file_path = os.path.join(UPLOAD_DIR,'제품별공정별소요자재조회_최종차수.csv')

# 최종 결과를 새 CSV 파일로 저장
df_final_process.to_csv(new_file_path, index=False, encoding='utf-8-sig')

print("최종차수 기준의 BOM 리스트 만들기 작업이 완료되었습니다.")

# 4단계_BOM ------------------------------------------------------------------------------------------------------------------------------------------

import pandas as pd
import os

# 현재 스크립트의 디렉토리를 작업 디렉토리로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)

# '제품별공정별소요자재조회.csv'에서 헤더 정보 가져오기
source_file_path = os.path.join(UPLOAD_DIR,'제품별공정별소요자재조회.csv')
df_source = pd.read_csv(source_file_path, nrows=0)  # 데이터를 불러오지 않고 헤더만 읽어옴
headers = df_source.columns.tolist()  # 헤더 정보를 리스트로 변환

# 신규 파일 목록
new_file_names = ['최종차수_구매_BOM.csv', '최종차수_제작_BOM_001.csv']

# 신규 파일 생성 및 헤더 설정
for file_name in new_file_names:
    # 새 DataFrame 생성 및 헤더 설정
    df_new = pd.DataFrame(columns=headers)
    
    # 신규 파일 경로 설정 (uploads/ 폴더에 저장)
    new_file_path = os.path.join(UPLOAD_DIR, file_name)  # `UPLOAD_DIR`을 `uploads/`로 지정
    
    # 신규 CSV 파일로 저장
    df_new.to_csv(new_file_path, index=False, encoding='utf-8-sig')

print("신규 파일 생성 및 헤더 설정 작업이 완료되었습니다.")

# 5단계_BOM ------------------------------------------------------------------------------------------------------------------------------------------

import pandas as pd
import os

# 현재 스크립트의 디렉토리를 작업 디렉토리로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)

# '제품별공정별소요자재조회_최종차수.csv'에서 데이터 로드
source_file_path = os.path.join(UPLOAD_DIR,'제품별공정별소요자재조회_최종차수.csv')
df_final_bom = pd.read_csv(source_file_path)

# '조달구분' 열 정보가 '구매'인 행만 필터링
df_purchase_bom = df_final_bom[df_final_bom['조달구분'] == '구매']

# '조달구분' 열 정보가 '제작'인 행만 필터링
df_manufacture_bom = df_final_bom[df_final_bom['조달구분'] == '제작']

# 각 필터링된 데이터를 해당하는 CSV 파일에 저장
purchase_file_path = os.path.join(UPLOAD_DIR,'최종차수_구매_BOM.csv')
manufacture_file_path = os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_001.csv')

df_purchase_bom.to_csv(purchase_file_path, index=False, encoding='utf-8-sig')
df_manufacture_bom.to_csv(manufacture_file_path, index=False, encoding='utf-8-sig')

print("최종차수 기준의 BOM 리스트 데이터 채우기 작업이 완료되었습니다.")

# 6단계_BOM ------------------------------------------------------------------------------------------------------------------------------------------

import pandas as pd
import os

# 현재 스크립트의 디렉토리를 작업 디렉토리로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)

# '최종차수_제작_BOM_001.csv' 파일에서 헤더 정보 읽기
file_path_001 = os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_001.csv')
df_001 = pd.read_csv(file_path_001, nrows=0)  # 데이터를 로드하지 않고 헤더만 가져옵니다
headers = df_001.columns.tolist()  # 헤더 정보를 리스트로 변환

# 'BOM환산수량' 헤더 추가 (필요에 따라 추가)
headers.append('BOM환산수량')

# 새 DataFrame 생성 및 CSV 파일로 저장
file_path_002 = os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_002.csv')
df_002 = pd.DataFrame(columns=headers)  # 새로운 헤더를 가진 빈 DataFrame 생성
df_002.to_csv(file_path_002, index=False, encoding='utf-8-sig')  # 파일 저장

print('최종차수_제작_BOM_002 작업이 완료되었습니다.')

# 7단계_BOM ------------------------------------------------------------------------------------------------------------------------------------------

import pandas as pd
import os

# 현재 스크립트의 디렉토리를 작업 디렉토리로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)

# 파일 경로 설정
file_path = os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_001.csv')
file_path2 = os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_002.csv')
file_path3 = os.path.join(UPLOAD_DIR,'제품별공정별소요자재조회_최종차수.csv')

# 엑셀 파일 로드
df_bom_001 = pd.read_csv(file_path)
df_materials = pd.read_csv(file_path3)

# 유지해야 하는 기존 정보 헤더 목록
retain_headers = ['품목자산분류', '품명', '품번', '규격', '단위', 'BOM차수', 'BOM차수명', '공정흐름차수', '공정흐름차수명', '공정', '공정품명', '공정품번', '공정품규격', '단위.1', '공정품소요량']

# 추가해야 하는 자재 정보 헤더 목록
add_headers = ['자재명', '자재번호', '자재규격', '단위.2', '소요량분자', '소요량분모', '소요량', '내부Loss율', '내부Loss율반영 소요량', '외부Loss율', '외부Loss율반영 소요량', '구매단가', '구매금액', '현재고', '대표거래처', '비고', '조달구분', '품목소분류']

# 결과 저장을 위한 빈 리스트
expanded_data = []

# 조달구분이 '제작'인 행을 식별하고 자재 정보를 추가
for _, row in df_bom_001.iterrows():
    if row['조달구분'] == '제작':
        material_number = row['자재번호']
        materials = df_materials[df_materials['품번'] == material_number]
        
        for _, mat in materials.iterrows():
            # 기존 정보 유지
            new_row = {header: row[header] for header in retain_headers}
            # 새로운 자재 정보 추가
            for header in add_headers:
                new_row[header] = mat.get(header, None)
            # BOM환산수량 계산 (예제로, 실제 계산 방식에 따라 조정 필요)
            new_row['BOM환산수량'] = row.get('소요량분자', 1) * mat.get('소요량분자', 1)
            expanded_data.append(new_row)

# 확장된 데이터를 DataFrame으로 변환
df_expanded = pd.DataFrame(expanded_data)

# 최종 결과를 새 CSV 파일로 저장
df_expanded.to_csv(file_path2, index=False, encoding='utf-8-sig')

print('작업이 완료되었습니다.')

# 8단계_BOM ------------------------------------------------------------------------------------------------------------------------------------------

import pandas as pd
import os

# 현재 스크립트의 디렉토리를 작업 디렉토리로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)

# '최종차수_제작_BOM_001.csv' 파일에서 헤더 정보 읽기
file_path_001 = os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_002.csv')
df_001 = pd.read_csv(file_path_001, nrows=0)  # 데이터를 로드하지 않고 헤더만 가져옵니다
headers = df_001.columns.tolist()  # 헤더 정보를 리스트로 변환

# 'BOM환산수량' 헤더 추가 (필요에 따라 추가)
headers.append('BOM환산수량')

# 새 DataFrame 생성 및 CSV 파일로 저장
file_path_002 = os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_003.csv')
df_002 = pd.DataFrame(columns=headers)  # 새로운 헤더를 가진 빈 DataFrame 생성
df_002.to_csv(file_path_002, index=False, encoding='utf-8-sig')  # 파일 저장

print('최종차수_제작_BOM_003 작업이 완료되었습니다.')

# 9단계_BOM ------------------------------------------------------------------------------------------------------------------------------------------

import pandas as pd
import os

# 현재 스크립트의 디렉토리를 작업 디렉토리로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)

# 파일 경로 설정
file_path = os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_002.csv')
file_path2 = os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_003.csv')
file_path3 = os.path.join(UPLOAD_DIR,'제품별공정별소요자재조회_최종차수.csv')

# 시트 데이터 로드
bom_df = pd.read_csv(file_path)
materials_df = pd.read_csv(file_path3)

# 자재번호와 품번의 형식 표준화
bom_df['자재번호'] = bom_df['자재번호'].astype(str).str.strip().str.upper()
materials_df['품번'] = materials_df['품번'].astype(str).str.strip().str.upper()

# 유지해야 되는 기존 정보 헤더 범위와 확장해야 되는 정보 헤더 범위
maintain_headers = ['품목자산분류', '품명', '품번', '규격', '단위', 'BOM차수', 'BOM차수명', '공정흐름차수', '공정흐름차수명', '공정', '공정품명', '공정품번', '공정품규격', '단위.1', '공정품소요량']
expand_headers = ['자재명', '자재번호', '자재규격', '단위.2', '소요량분자', '소요량분모', '소요량', '내부Loss율', '내부Loss율반영 소요량', '외부Loss율', '외부Loss율반영 소요량', '구매단가', '구매금액', '현재고', '대표거래처', '비고', '조달구분', '품목소분류']

# 데이터 처리
expanded_data_rows = []

for index, row in bom_df.iterrows():
    if row['조달구분'] == '제작':
        material_info = materials_df[materials_df['품번'] == row['자재번호']]
        for _, mat_row in material_info.iterrows():
            combined_row = {col: row[col] for col in maintain_headers}
            combined_row.update({col: mat_row[col] for col in expand_headers if col in mat_row})
            combined_row['BOM환산수량'] = row['BOM환산수량'] * mat_row.get('소요량분자', 1)
            expanded_data_rows.append(combined_row)
    else:  # '구매'인 경우
        combined_row = {col: row[col] for col in bom_df.columns if col in row}
        expanded_data_rows.append(combined_row)

# 확장된 데이터 DataFrame 생성
expanded_data = pd.DataFrame(expanded_data_rows)

# 최종 결과를 새 CSV 파일로 저장
expanded_data.to_csv(file_path2, index=False, encoding='utf-8-sig')

print("작업이 완료되었습니다")

# 10단계_BOM ------------------------------------------------------------------------------------------------------------------------------------------

import pandas as pd
import os

# 현재 스크립트의 디렉토리를 작업 디렉토리로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)

# '최종차수_제작_BOM_001.csv' 파일에서 헤더 정보 읽기
file_path_001 = os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_003.csv')
df_001 = pd.read_csv(file_path_001, nrows=0)  # 데이터를 로드하지 않고 헤더만 가져옵니다
headers = df_001.columns.tolist()  # 헤더 정보를 리스트로 변환

# 'BOM환산수량' 헤더 추가 (필요에 따라 추가)
headers.append('BOM환산수량')

# 새 DataFrame 생성 및 CSV 파일로 저장
file_path_002 = os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_004.csv')
df_002 = pd.DataFrame(columns=headers)  # 새로운 헤더를 가진 빈 DataFrame 생성
df_002.to_csv(file_path_002, index=False, encoding='utf-8-sig')  # 파일 저장

print('최종차수_제작_BOM_004 작업이 완료되었습니다.')

# 11단계_BOM ------------------------------------------------------------------------------------------------------------------------------------------

import pandas as pd
import os

# 현재 스크립트의 디렉토리를 작업 디렉토리로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)

# 파일 경로 설정
file_path = os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_003.csv')
file_path2 = os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_004.csv')
file_path3 = os.path.join(UPLOAD_DIR,'제품별공정별소요자재조회_최종차수.csv')

# 시트 데이터 로드
bom_df = pd.read_csv(file_path)
materials_df = pd.read_csv(file_path3)

# 자재번호와 품번의 형식 표준화
bom_df['자재번호'] = bom_df['자재번호'].astype(str).str.strip().str.upper()
materials_df['품번'] = materials_df['품번'].astype(str).str.strip().str.upper()

# 유지해야 되는 기존 정보 헤더 범위와 확장해야 되는 정보 헤더 범위
maintain_headers = ['품목자산분류', '품명', '품번', '규격', '단위', 'BOM차수', 'BOM차수명', '공정흐름차수', '공정흐름차수명', '공정', '공정품명', '공정품번', '공정품규격', '단위.1', '공정품소요량']
expand_headers = ['자재명', '자재번호', '자재규격', '단위.2', '소요량분자', '소요량분모', '소요량', '내부Loss율', '내부Loss율반영 소요량', '외부Loss율', '외부Loss율반영 소요량', '구매단가', '구매금액', '현재고', '대표거래처', '비고', '조달구분', '품목소분류']

# 데이터 처리
expanded_data_rows = []

for index, row in bom_df.iterrows():
    if row['조달구분'] == '제작':
        material_info = materials_df[materials_df['품번'] == row['자재번호']]
        for _, mat_row in material_info.iterrows():
            combined_row = {col: row[col] for col in maintain_headers}
            combined_row.update({col: mat_row[col] for col in expand_headers if col in mat_row})
            combined_row['BOM환산수량'] = row['BOM환산수량'] * mat_row.get('소요량분자', 1)
            expanded_data_rows.append(combined_row)
    else:  # '구매'인 경우
        combined_row = {col: row[col] for col in bom_df.columns if col in row}
        expanded_data_rows.append(combined_row)

# 확장된 데이터 DataFrame 생성
expanded_data = pd.DataFrame(expanded_data_rows)

# 최종 결과를 새 CSV 파일로 저장
expanded_data.to_csv(file_path2, index=False, encoding='utf-8-sig')

print("작업이 완료되었습니다")

# 12단계_BOM ------------------------------------------------------------------------------------------------------------------------------------------

import pandas as pd
import os

# 현재 스크립트의 디렉토리를 작업 디렉토리로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)

# '최종차수_제작_BOM_001.csv' 파일에서 헤더 정보 읽기
file_path_001 = os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_004.csv')
df_001 = pd.read_csv(file_path_001, nrows=0)  # 데이터를 로드하지 않고 헤더만 가져옵니다
headers = df_001.columns.tolist()  # 헤더 정보를 리스트로 변환

# 'BOM환산수량' 헤더 추가 (필요에 따라 추가)
headers.append('BOM환산수량')

# 새 DataFrame 생성 및 CSV 파일로 저장
file_path_002 = os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_005.csv')
df_002 = pd.DataFrame(columns=headers)  # 새로운 헤더를 가진 빈 DataFrame 생성
df_002.to_csv(file_path_002, index=False, encoding='utf-8-sig')  # 파일 저장

print('최종차수_제작_BOM_004 작업이 완료되었습니다.')

# 13단계_BOM ------------------------------------------------------------------------------------------------------------------------------------------

import pandas as pd
import os

# 현재 스크립트의 디렉토리를 작업 디렉토리로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)

# 파일 경로 설정
file_path = os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_004.csv')
file_path2 = os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_005.csv')
file_path3 = os.path.join(UPLOAD_DIR,'제품별공정별소요자재조회_최종차수.csv')

# 시트 데이터 로드
bom_df = pd.read_csv(file_path)
materials_df = pd.read_csv(file_path3)

# 자재번호와 품번의 형식 표준화
bom_df['자재번호'] = bom_df['자재번호'].astype(str).str.strip().str.upper()
materials_df['품번'] = materials_df['품번'].astype(str).str.strip().str.upper()

# 유지해야 되는 기존 정보 헤더 범위와 확장해야 되는 정보 헤더 범위
maintain_headers = ['품목자산분류', '품명', '품번', '규격', '단위', 'BOM차수', 'BOM차수명', '공정흐름차수', '공정흐름차수명', '공정', '공정품명', '공정품번', '공정품규격', '단위.1', '공정품소요량']
expand_headers = ['자재명', '자재번호', '자재규격', '단위.2', '소요량분자', '소요량분모', '소요량', '내부Loss율', '내부Loss율반영 소요량', '외부Loss율', '외부Loss율반영 소요량', '구매단가', '구매금액', '현재고', '대표거래처', '비고', '조달구분', '품목소분류']

# 데이터 처리
expanded_data_rows = []

for index, row in bom_df.iterrows():
    if row['조달구분'] == '제작':
        material_info = materials_df[materials_df['품번'] == row['자재번호']]
        for _, mat_row in material_info.iterrows():
            combined_row = {col: row[col] for col in maintain_headers}
            combined_row.update({col: mat_row[col] for col in expand_headers if col in mat_row})
            combined_row['BOM환산수량'] = row['BOM환산수량'] * mat_row.get('소요량분자', 1)
            expanded_data_rows.append(combined_row)
    else:  # '구매'인 경우
        combined_row = {col: row[col] for col in bom_df.columns if col in row}
        expanded_data_rows.append(combined_row)

# 확장된 데이터 DataFrame 생성
expanded_data = pd.DataFrame(expanded_data_rows)

# 최종 결과를 새 CSV 파일로 저장
expanded_data.to_csv(file_path2, index=False, encoding='utf-8-sig')

print("작업이 완료되었습니다")

# 14단계_BOM ------------------------------------------------------------------------------------------------------------------------------------------

import pandas as pd
import os

# 현재 스크립트의 디렉토리를 작업 디렉토리로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)

# CSV 파일을 불러옵니다.
구매_BOM_경로 = os.path.join(UPLOAD_DIR,'최종차수_구매_BOM.csv')
제작_BOM_경로 = os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_005.csv')

# 파일을 읽어옵니다.
구매_BOM = pd.read_csv(구매_BOM_경로, encoding='utf-8-sig')
제작_BOM = pd.read_csv(제작_BOM_경로, encoding='utf-8-sig')

# 'BOM환산수량' 헤더를 생성하고 '소요량분자'의 값을 복사합니다.
구매_BOM['BOM환산수량'] = 구매_BOM['소요량분자']

# 구매 BOM 파일을 기준으로 제작 BOM 파일의 헤더가 일치할 경우 값을 추가합니다.
합본_BOM = pd.concat([구매_BOM, 제작_BOM], ignore_index=True)

# 결과 파일을 'BOM.csv'로 저장합니다.
합본_BOM.to_csv(os.path.join(UPLOAD_DIR,'BOM.csv'), encoding='utf-8-sig', index=False)

print("'BOM.csv' 파일이 업데이트되었습니다.")
