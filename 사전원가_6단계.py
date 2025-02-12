import pandas as pd
import os
import logging

# 현재 스크립트 위치를 기준으로 작업 디렉토리 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

UPLOAD_DIR = "uploads"
RESULT_DIR = "results"

# 로그 설정
logging.basicConfig(filename="log_BOM_단가누락.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    try:
        # 파일 경로 설정
        bom_file = os.path.join(UPLOAD_DIR,'BOM_가공_액상,추출액.csv')

        # 파일 불러오기 (encoding='utf-8-sig')
        bom_df = pd.read_csv(bom_file, encoding='utf-8-sig', low_memory=False)

        # 1) '공정흐름차수명' 필터링 및 '품번' 추출
        target_processes = ['노무비', '임가공비', '재료비', '제조경비']
        if '공정흐름차수명' not in bom_df.columns:
            raise KeyError("'공정흐름차수명' 열이 BOM 데이터에 존재하지 않습니다.")
        
        filtered_bom = bom_df[bom_df['공정흐름차수명'].isin(target_processes)]
        if filtered_bom.empty:
            logging.warning("필터링된 데이터가 없습니다. '공정흐름차수명' 값을 확인하세요.")
        else:
            logging.info(f"필터링된 데이터 수: {len(filtered_bom)}")
        
        unique_parts = filtered_bom[['품번']].drop_duplicates()
        logging.info(f"공정흐름차수명 필터링 완료 - 추출된 품번 수: {len(unique_parts)}")

        # 2) '단가'가 0이거나 빈칸인 경우의 행 추출 및 저장
        if '단가' not in bom_df.columns:
            raise KeyError("'단가' 열이 BOM 데이터에 존재하지 않습니다.")
        
        # '단가'가 0 또는 NaN이고, 1)에서 추출된 '품번'과 동일한 행만 필터링
        cost_error_df = bom_df[
            (bom_df['단가'].isna() | (bom_df['단가'] == 0)) & 
            (bom_df['품번'].isin(unique_parts['품번']))
        ]

        if cost_error_df.empty:
            logging.warning("단가가 0이거나 빈칸인 데이터가 없습니다.")
        else:
            logging.info(f"단가가 0이거나 빈칸인 데이터 수: {len(cost_error_df)}")
        
        # 3) '단가 오류.csv'에서 특정 조건으로 데이터 필터링
        filtered_cost_error_df = cost_error_df[
            (cost_error_df['품목대분류'] != '반제품') &
            (cost_error_df['품목자산분류'].isin(['원자재', '부자재'])) &
            (~cost_error_df['자재명'].isin(['정제수','이산화탄소','청보리순'])) &
            (cost_error_df['자재번호'] != '51A92003') & #관능용으로 단가가 없음(레브)
            (~cost_error_df['자재명'].str.contains('이마트|트레이', na=False))  # 자재명에 '이마트' 또는 '트레이'가 포함되지 않은 경우
        ]
        
        if filtered_cost_error_df.empty:
            logging.warning("조건에 맞는 데이터가 없습니다. '품목대분류'와 '품목자산분류' 값을 확인하세요.")
        else:
            logging.info(f"조건에 맞는 데이터 수: {len(filtered_cost_error_df)}")
        
        # 결과 저장
        columns_to_save = ['품목대분류', '품번', '품명', '자재번호', '자재명', '단가']
        if not all(col in filtered_cost_error_df.columns for col in columns_to_save):
            missing_cols = [col for col in columns_to_save if col not in filtered_cost_error_df.columns]
            logging.error(f"저장하려는 열 중 누락된 열이 있습니다: {missing_cols}")
            print(f"오류: 저장하려는 열 중 누락된 열이 있습니다: {missing_cols}")
        else:
            filtered_cost_error_df = filtered_cost_error_df[columns_to_save]
            excel_path = os.path.join(RESULT_DIR, '단가누락.xlsx')
            filtered_cost_error_df.to_excel(excel_path, index=False, engine='openpyxl')
            print("BOM 내 단가 누락 파일 추출 완료 파일명: '단가누락.xlsx'")
            logging.info(f"최종 필터링된 데이터를 '단가누락.xlsx' 파일로 저장 완료 - 저장된 행 수: {len(filtered_cost_error_df)}")

    except FileNotFoundError as e:
        logging.error(f"파일을 찾을 수 없습니다: {e}")
        print("오류: 파일을 찾을 수 없습니다.")
    except KeyError as e:
        logging.error(f"필수 열이 누락되었습니다: {e}")
        print("오류: 필수 열이 누락되었습니다. 로그 파일을 확인하세요.")
    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {e}")
        print("오류 발생: 로그 파일을 확인하세요.")

if __name__ == "__main__":
    main()