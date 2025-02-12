import pandas as pd
import os
import numpy as np
import logging

# 현재 스크립트 위치를 기준으로 작업 디렉토리 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

UPLOAD_DIR = "uploads"

# 로그 설정
logging.basicConfig(filename="log_BOM_추출_액상,추출액.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    try:
        # 파일 경로 설정
        bom_file = os.path.join(UPLOAD_DIR,'BOM_추출_액상,추출액.csv')

        # 파일 불러오기
        bom_df = pd.read_csv(bom_file, encoding='utf-8-sig', low_memory=False)
        logging.info("BOM 파일을 성공적으로 불러왔습니다.")

        # 1) 자재번호와 품번을 문자열로 변환
        bom_df['자재번호'] = bom_df['자재번호'].astype(str)
        bom_df['품번'] = bom_df['품번'].astype(str)
        logging.info("자재번호와 품번을 문자열로 변환 완료")

        # 2) 추출 공정 및 loss율_추출이 0이 아닌 품번 추출
        추출_품번 = bom_df[(bom_df['공정'] == '추출') & (bom_df['loss율_추출'] != 0)]['품번'].unique()
        logging.info(f"추출 대상 품번: {추출_품번}")

        # 3) 품번별 합산값 계산 (천안)
        천안_합산 = {}
        for 품번 in 추출_품번:
            # 조건: '공정'이 '추출'이고 'loss율_추출'이 0이 아닌 데이터
            필터 = bom_df[(bom_df['품번'] == 품번) & (bom_df['공정'] == '추출') & (bom_df['loss율_추출'] != 0)]

            if not 필터.empty:
                천안_합산[품번] = (
                    필터['수율_추출'].iloc[0] +
                    필터['loss율_추출'].iloc[0] +
                    필터['추출_routing'].iloc[0]
                )
            else:
                천안_합산[품번] = 0
            logging.info(f"천안 합산 - 품번: {품번}, 합산값: {천안_합산[품번]}")
            
        # 4) 품번별 합산값 계산 (외주)
        외주_합산 = {}
        for 품번 in 추출_품번:
            # 조건: '공정'이 '추출'이고 'loss율_추출'이 0이 아닌 데이터
            필터 = bom_df[(bom_df['품번'] == 품번) & (bom_df['공정'] == '추출') & (bom_df['loss율_추출'] != 0)]

            if not 필터.empty:
                외주_합산[품번] = (
                    필터['수율_추출'].iloc[0] +
                    필터['loss율_추출'].iloc[0] +
                    필터['추출_routing_외주'].iloc[0]
                )
            else:
                외주_합산[품번] = 0
            logging.info(f"외주 합산 - 품번: {품번}, 합산값: {외주_합산[품번]}")


        # 5) 단가_추출_천안 열 생성
        천안_공정 = ['미세', '배/착', '배전', '배합', '분/착', '분쇄', '제/배', '포장']
        bom_df['단가_추출_천안'] = 0.0

        # 자재번호와 추출_품번 비교 및 값 할당
        for 자재번호 in bom_df['자재번호'].unique():
            if 자재번호 in 추출_품번:
                mask = (bom_df['자재번호'] == 자재번호) & (bom_df['공정'].isin(천안_공정))
                bom_df.loc[mask, '단가_추출_천안'] = 천안_합산.get(자재번호, 0)
        logging.info("단가_추출_천안 열 생성 및 값 할당 완료")

        # 6) 단가_추출_외주 열 생성
        bom_df['단가_추출_외주'] = 0.0

        # 자재번호와 추출_품번 비교 및 값 할당
        for 자재번호 in bom_df['자재번호'].unique():
            if 자재번호 in 추출_품번:
                mask = (bom_df['자재번호'] == 자재번호) & (~bom_df['공정'].isin(천안_공정))
                bom_df.loc[mask, '단가_추출_외주'] = 외주_합산.get(자재번호, 0)
        logging.info("단가_추출_외주 열 생성 및 값 할당 완료")

        # '공정'이 '추출'인 값에서 '품번', '자재번호', '단가','비고','조달구분' 중복값을 제거
        bom_df = bom_df[~(bom_df.duplicated(subset=['품번', '자재번호', '단가', '비고', '조달구분','단가_추출_천안','단가_추출_외주']))]
        logging.info("공정이 '추출'인 값에서 '품번', '자재번호', '단가', '비고', '조달구분','단가_추출_천안','단가_추출_외주' 중복값 제거 완료.")

        # 결과 저장
        bom_df.to_csv(bom_file, index=False, encoding='utf-8-sig')
        print("BOM 추출 작업 완료 - BOM_추출_액상,추출액.csv 파일 갱신 완료")
        logging.info("BOM 추출 작업이 완료되었습니다.")

    except FileNotFoundError as e:
        logging.error(f"파일을 찾을 수 없습니다: {e}")
        print("오류: 파일을 찾을 수 없습니다.")
    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {e}")
        print(f"오류 발생: {e}")
        print("자세한 내용은 로그 파일을 확인하세요.")

if __name__ == "__main__":
    main()

