import os
import pandas as pd
import logging
import numpy as np

# 현재 스크립트 위치를 기준으로 작업 디렉토리 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

UPLOAD_DIR = "uploads"

# 로그 설정
logging.basicConfig(filename="log_BOM_미세_조제.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    try:
        # 파일 경로 설정
        bom_file = os.path.join(UPLOAD_DIR,'BOM_배전_조제.csv')

        # 파일 불러오기 (encoding='utf-8-sig')
        bom_df = pd.read_csv(bom_file, encoding='utf-8-sig', low_memory=False)
        logging.info("BOM 파일을 성공적으로 불러왔습니다.")

        # '품목대분류'가 '조제'이고 '자재번호'가 특정 값인 경우 행 삭제
        삭제_조건 = (bom_df['품목대분류'] == '조제') & (bom_df['자재번호'].isin(['62C00030', '64C00060']))
        bom_df = bom_df[~삭제_조건]
        logging.info("'품목대분류'가 '조제'이고 '자재번호'가 '62C00030', '64C00060'인 행 삭제 완료.")

        # '수율_미세' 열 생성 및 '공정'이 '미세'일 때 처리
        bom_df['수율_미세'] = np.where(
            bom_df['공정'] == '미세', 
            bom_df['환산비용'] / bom_df['수율'].replace(0, np.nan), 
            0
        )
        bom_df['수율_미세'] = bom_df['수율_미세'].fillna(0).astype(float)
        logging.info("수율_미세 열 생성 완료.")

        # 'loss율_미세' 열 생성 및 '공정'이 '미세'일 때 처리
        bom_df['loss율_미세'] = np.where(
            bom_df['공정'] == '미세', 
            bom_df['수율_미세'] * bom_df['loss율'], 
            0
        )
        bom_df['loss율_미세'] = bom_df['loss율_미세'].fillna(0).astype(float)
        logging.info("loss율_미세 열 생성 완료.")

        # 결과 저장
        output_file = os.path.join(UPLOAD_DIR,'BOM_미세_조제.csv')
        bom_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print("BOM 미세 가공 완료 - BOM_미세.csv 파일 갱신 완료")
        logging.info("BOM 미세 가공 작업이 완료되었습니다.")

    except FileNotFoundError as e:
        logging.error(f"파일을 찾을 수 없습니다: {e}")
        print("오류: 파일을 찾을 수 없습니다.")
    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {e}")
        print("오류 발생, 로그 파일을 확인하세요.")

if __name__ == "__main__":
    main()
