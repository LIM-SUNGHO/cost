import pandas as pd
import os
import logging

# 현재 스크립트 위치를 기준으로 작업 디렉토리 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

UPLOAD_DIR = "uploads"

# 로그 설정
logging.basicConfig(filename="log_BOM_배전_조제.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    try:
        # 파일 경로 설정
        bom_file = os.path.join(UPLOAD_DIR,'BOM_가공_조제.csv')
        yield_file = os.path.join(UPLOAD_DIR,'수율.csv')

        # 파일 불러오기 (encoding='utf-8-sig')
        bom_df = pd.read_csv(bom_file, encoding='utf-8-sig', low_memory=False)
        yield_df = pd.read_csv(yield_file, encoding='utf-8-sig', low_memory=False)

        # '원자재' 구분 데이터만 포함한 yield_df와 병합 (필요한 열만 병합)
        yield_filtered = yield_df[yield_df['구분'] == '원자재'][['품번', '수율', 'loss율']]
        merged_df = pd.merge(bom_df, yield_filtered, on='품번', how='left')

        # 1. '수율_배전' 계산: '공정'이 '배전'인 경우 조건을 만족하면 계산, 그렇지 않으면 NaN 할당
        merged_df['수율_배전'] = merged_df.apply(
            lambda row: row['환산비용'] / row['수율'] 
            if row['공정'] == '배전' and pd.notna(row['수율']) and row['수율'] != 0 
            else None,
            axis=1
        )

        # 2. 'loss율_배전' 계산: '공정'이 '배전'인 경우 조건을 만족하면 계산, 그렇지 않으면 NaN 할당
        merged_df['loss율_배전'] = merged_df.apply(
            lambda row: row['수율_배전'] * row['loss율'] if row['공정'] == '배전' and pd.notna(row['loss율']) else None,
            axis=1
        )
        logging.info("수율_배전 및 loss율_배전 계산이 완료되었습니다.")

        # '공정흐름차수명'이 '노무비', '제조경비', '임가공비'인 경우 'loss율' 값을 제거
        target_values = ['노무비', '제조경비', '임가공비']
        merged_df.loc[merged_df['공정흐름차수명'].isin(target_values), 'loss율'] = None

        # '공정흐름차수명'이 '노무비', '제조경비', '임가공비'인 경우 'loss율' 값을 제거
        target_values = ['노무비', '제조경비', '임가공비']
        merged_df.loc[merged_df['공정흐름차수명'].isin(target_values), '수율'] = None
        logging.info("'공정흐름차수명'이 '노무비', '제조경비', '임가공비'인 경우 'loss율','수율' 제거 완료")

        # 1. '자재명'에 '건조' 또는 '열풍'이 포함된 행에서 '품번' 값 추출
        filtered_parts = merged_df.loc[merged_df['자재명'].str.contains('건조|열풍', na=False), '품번'].unique()

        # 2. 해당 '품번' 값 중 'loss율'이 0.1 또는 0.2인 행 삭제
        condition = (merged_df['품번'].isin(filtered_parts)) & (merged_df['loss율'].isin([0.1, 0.2]))
        merged_df = merged_df[~condition]

        logging.info("'건조' 또는 '열풍'이 포함된 자재명에서 추출된 품번 중 loss율이 0.1 또는 0.2인 행 삭제 완료")

        # 3. 결과를 'BOM_배전_조제.csv' 파일로 저장
        output_file = os.path.join(UPLOAD_DIR,'BOM_배전_조제.csv')
        merged_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print("BOM 배전 완료 - BOM_배전_조제.csv 파일 갱신 완료")
        logging.info("BOM 배전 작업이 완료되었습니다.")

    except FileNotFoundError as e:
        logging.error(f"파일을 찾을 수 없습니다: {e}")
        print("오류: 파일을 찾을 수 없습니다.")
    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {e}")
        print("오류 발생, 로그 파일을 확인하세요.")

if __name__ == "__main__":
    main()
