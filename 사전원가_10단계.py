import pandas as pd
import os
import logging

# 현재 스크립트 위치를 기준으로 작업 디렉토리 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

UPLOAD_DIR = "uploads"

# 로그 설정
logging.basicConfig(filename="log_BOM_분쇄_액상,추출액.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    try:
        # 파일 경로 설정
        bom_file = os.path.join(UPLOAD_DIR,'BOM_분쇄_액상,추출액.csv')

        # 파일 불러오기 (encoding='utf-8-sig')
        bom_df = pd.read_csv(bom_file, encoding='utf-8-sig', low_memory=False)

        # 데이터 타입 및 결측값 검사
        bom_df.fillna(0, inplace=True)
        bom_df['품번'] = bom_df['품번'].astype(str)
        bom_df['수율_분쇄'] = bom_df['수율_분쇄'].astype(float)
        bom_df['loss율_분쇄'] = bom_df['loss율_분쇄'].astype(float)
        bom_df['단가'] = bom_df['단가'].astype(float)

        # '분쇄비용' 컬럼 생성 및 값 계산
        bom_df['분쇄비용'] = 0.0

        # 1) '공정'이 '분쇄'인 행에 대해 '수율_분쇄'과 'loss율_분쇄'의 합산 값을 '분쇄비용'에 할당
        bom_df.loc[bom_df['공정'] == '분쇄', '분쇄비용'] = bom_df['수율_분쇄'] + bom_df['loss율_분쇄']

        # 2) '공정'이 '분쇄', '해포'이고 '공정흐름차수명'이 '노무비' 또는 '제조경비'일 때 동일한 '품번'마다의 '단가' 값을 '분쇄비용'에 할당
        bom_df.loc[
            (bom_df['공정'].isin(['분쇄', '해포'])) & (bom_df['공정흐름차수명'].isin(['노무비', '제조경비'])),
            '분쇄비용'
        ] = bom_df['단가']


        logging.info("분쇄비용 계산이 완료되었습니다.")

        # 결과를 'BOM_가공.csv' 파일로 저장
        bom_df.to_csv(bom_file, index=False, encoding='utf-8-sig')
        print("BOM 분쇄 가공 완료 - BOM_분쇄_액상,추출액.csv 파일 갱신 완료")
        logging.info("BOM 분쇄 가공 작업이 완료되었습니다.")

    except FileNotFoundError as e:
        logging.error(f"파일을 찾을 수 없습니다: {e}")
        print("오류: 파일을 찾을 수 없습니다.")
    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {e}")
        print("오류 발생, 로그 파일을 확인하세요.")

if __name__ == "__main__":
    main()
