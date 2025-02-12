import pandas as pd
import os
import logging

# 현재 스크립트 위치를 기준으로 작업 디렉토리 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

UPLOAD_DIR = "uploads"

# 로그 설정
logging.basicConfig(filename="log_BOM_가공_조제.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def convert_columns_to_string(df, columns, file_name):
    """
    지정된 DataFrame의 특정 열을 문자열로 변환
    """
    for column in columns:
        if column in df.columns:
            df[column] = df[column].astype(str).str.strip()
            logging.info(f"'{file_name}' 파일의 '{column}' 열을 문자열로 변환했습니다.")
        else:
            logging.warning(f"'{file_name}' 파일에 '{column}' 열이 없습니다.")

def main():
    try:
        # 파일 경로 설정
        bom_file = os.path.join(UPLOAD_DIR,'BOM_가공.csv')
        품목_조회_file = os.path.join(UPLOAD_DIR,'품목조회(추가정보).csv')
        자재_조회_file = os.path.join(UPLOAD_DIR,'자재조회(추가정보).csv')
        raw_cost_file = os.path.join(UPLOAD_DIR,'raw_사전원가.csv')

        # 파일 불러오기
        bom_df = pd.read_csv(bom_file, encoding='utf-8-sig', low_memory=False)
        품목_조회_df = pd.read_csv(품목_조회_file, encoding='utf-8-sig', low_memory=False)
        자재_조회_df = pd.read_csv(자재_조회_file, encoding='utf-8-sig', low_memory=False)
        raw_cost_df = pd.read_csv(raw_cost_file, encoding='utf-8-sig', low_memory=False)
        logging.info("모든 파일을 성공적으로 불러왔습니다.")

        # '품번'과 '자재번호'를 문자열로 변환
        convert_columns_to_string(bom_df, ['품번', '자재번호'], bom_file)
        convert_columns_to_string(품목_조회_df, ['품번'], 품목_조회_file)
        convert_columns_to_string(자재_조회_df, ['자재번호'], 자재_조회_file)
        convert_columns_to_string(raw_cost_df, ['품번'], raw_cost_file)

        # 1) raw_사전원가.csv 데이터를 결합
        # 필요한 열만 선택 및 열 이름 변경
        raw_cost_df = raw_cost_df[['품명', '품번', '공정', '항목', '합계', '작업단계', '구분', '구분.1']]
        raw_cost_df = raw_cost_df.rename(columns={
            '합계': '단가',
            '항목': '공정흐름차수명',
            '작업단계': '조달구분',
            '구분': '비고',
            '구분.1': '규격'
        })

        # 특정 항목 값을 '공정' 열로 이동
        target_items = ['비닐', '박스(30kg)', '재활용분담금', '스티커', '트레이더스']

        # 조건에 맞는 항목을 공정 열로 이동하고, 공정흐름차수명은 공백 처리
        raw_cost_df.loc[raw_cost_df['공정흐름차수명'].isin(target_items), '공정'] = raw_cost_df['공정흐름차수명']
        raw_cost_df.loc[raw_cost_df['공정흐름차수명'].isin(target_items), '공정흐름차수명'] = ''

        # BOM 데이터와 raw_사전원가 데이터 결합
        combined_df = pd.concat([bom_df, raw_cost_df], ignore_index=True)
        logging.info("raw_사전원가 데이터를 BOM 데이터에 결합했습니다.")

        # 2) '품목대분류' 열 생성 및 값 매핑
        if '품목대분류' not in combined_df.columns:
            combined_df['품목대분류'] = pd.NA  # '품목대분류' 열 생성
        품목_매핑 = 품목_조회_df.set_index('품번')['품목대분류']
        combined_df['품목대분류'] = combined_df['품번'].map(품목_매핑)
        logging.info("'품목대분류' 열을 생성하고 값 매핑을 완료했습니다.")

        # 3) '품목자산분류' 열 생성 및 값 매핑
        if '품목자산분류' not in combined_df.columns:
            combined_df['품목자산분류'] = pd.NA  # '품목자산분류' 열 생성
        품목자산분류_매핑 = 자재_조회_df.set_index('자재번호')['품목자산분류']
        combined_df['품목자산분류'] = combined_df['자재번호'].map(품목자산분류_매핑)
        logging.info("'품목자산분류' 열을 생성하고 값 매핑을 완료했습니다.")

        # 4) 열 순서 재배치
        # '품목대분류'를 '품명' 앞에 위치
        품목_대분류_idx = combined_df.columns.get_loc('품명')
        cols = combined_df.columns.tolist()
        cols.insert(품목_대분류_idx, cols.pop(cols.index('품목대분류')))

        # '품목자산분류'를 '자재명' 앞에 위치
        자재명_idx = cols.index('자재명')
        cols.insert(자재명_idx, cols.pop(cols.index('품목자산분류')))

        combined_df = combined_df[cols]
        logging.info("열 순서를 재배치했습니다.")

        # 2-6. '구분' 열 생성 및 조건부 값 설정
        if '구분' not in combined_df.columns:
            combined_df['구분'] = pd.NA  # '구분' 열 생성

        # '자재명'에 '미세-' 값이 있는 경우 해당 '품번'의 모든 행에 '구분' 값을 '미세'로 설정
        if '자재명' in combined_df.columns and '품번' in combined_df.columns:
            misen_items = combined_df[combined_df['자재명'].str.contains('미세-', na=False)]['품번'].unique()
            combined_df.loc[combined_df['품번'].isin(misen_items), '구분'] = '미세'
            logging.info("'자재명'에 '미세-'가 포함된 '품번'에 대해 '구분' 값을 '미세'로 설정했습니다.")
            
            # '품번'이 '23151003'인 경우 '구분' 값을 빈칸으로 설정
            combined_df.loc[combined_df['품번'] == '23151003', '구분'] = ''
            logging.info("'품번'이 '23151003'인 경우 '구분' 값을 빈칸으로 설정했습니다.")
            
        # '품목대분류'가 '조제'이고 '품명'에 '세트'가 포함된 경우 '구분' 값을 '선물세트'로 설정
        if '품목대분류' in combined_df.columns and '품명' in combined_df.columns:
            condition = (combined_df['품목대분류'] == '조제') & combined_df['품명'].str.contains('세트', na=False)
            combined_df.loc[condition, '구분'] = '선물세트'
            logging.info("'품목대분류'가 '조제'이고 '자재명'에 '세트'가 포함된 행에 대해 '구분' 값을 '선물세트'로 설정했습니다.")
            
        # '품명'이 '아워티 셀렉션'이면 '구분' 값을 '선물세트'로 설정
        if '품명' in combined_df.columns:
            condition = combined_df['품명'] == '아워티 셀렉션'
            combined_df.loc[condition, '구분'] = '선물세트'
            logging.info("'품명'이 '아워티 셀렉션'인 행에 대해 '구분' 값을 '선물세트'로 설정했습니다.")

        품목대분류_idx = combined_df.columns.get_loc('품목대분류')
        cols = combined_df.columns.tolist()
        cols.insert(품목대분류_idx, cols.pop(cols.index('구분')))
        
        combined_df2 = combined_df[cols]
        logging.info("열 순서를 재배치했습니다.")

        # 결과 저장
        output_file = os.path.join(UPLOAD_DIR,'BOM_가공_조제.csv')
        combined_df2.to_csv(output_file, index=False, encoding='utf-8-sig')
        print("BOM 가공 완료 - BOM.csv 파일 갱신 완료")
        logging.info("BOM 가공 작업이 완료되었습니다.")


    except FileNotFoundError as e:
        logging.error(f"파일을 찾을 수 없습니다: {e}")
        print("오류: 파일을 찾을 수 없습니다.")
    except KeyError as e:
        logging.error(f"필수 열이 누락되었습니다: {e}")
        print("오류: 필수 열이 누락되었습니다. 로그 파일을 확인하세요.")
    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {e}")
        print(f"오류 발생: {e}")
        print("자세한 내용은 로그 파일을 확인하세요.")

if __name__ == "__main__":
    main()
