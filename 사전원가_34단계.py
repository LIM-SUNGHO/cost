import pandas as pd
import numpy as np
import os
import logging

# 현재 스크립트 위치를 기준으로 작업 디렉토리 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

UPLOAD_DIR = "uploads"

# 로그 설정
logging.basicConfig(filename="log_BOM_스틱_원두.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    try:
        # 파일 경로 설정
        bom_file = os.path.join(UPLOAD_DIR,'BOM_스틱_원두.csv')

        # 파일 불러오기 (encoding='utf-8-sig')
        bom_df = pd.read_csv(bom_file, encoding='utf-8-sig', low_memory=False)

        # 데이터 타입 및 결측값 검사
        bom_df.fillna(0, inplace=True)
        bom_df['품번'] = bom_df['품번'].astype(str)
        bom_df['수율_스틱'] = bom_df['수율_스틱'].astype(float)
        bom_df['loss율_스틱'] = bom_df['loss율_스틱'].astype(float)
        bom_df['단가'] = bom_df['단가'].astype(float)

        # '스틱비용' 컬럼 생성 및 값 계산
        bom_df['스틱비용'] = 0.0

        # 1) '품명'에 '스틱'이 들어가 있는 경우
        condition_1 = bom_df['품명'].str.contains('스틱-')

        # '수율_스틱' 계산 조건
        bom_df.loc[condition_1, '수율_스틱'] = np.where(
            # '자재명'에 '분/착'이 포함되고 '단가_분착'이 0이 아닌 경우
            bom_df.loc[condition_1, '자재명'].str.contains('분/착') & (bom_df.loc[condition_1, '단가_분/착'] != 0),
            bom_df.loc[condition_1, '단가_분/착'] * bom_df.loc[condition_1, '소요량분자'],  # True일 경우 계산식

            # '자재명'에 '배전'이 포함되고 '단가_분착'이 0인 경우
            np.where(
                bom_df.loc[condition_1, '자재명'].str.contains('배전') & (bom_df.loc[condition_1, '단가_분/착'] == 0),
                bom_df.loc[condition_1, '단가_배전'] * bom_df.loc[condition_1, '소요량분자'],  # True일 경우 계산식
                0  # 위 조건이 모두 False일 경우 0
            )
        )

        # 'loss율_스틱' 계산
        bom_df.loc[condition_1, 'loss율_스틱'] = bom_df['수율_스틱'] * bom_df['loss율']

        # '스틱비용' 계산
        bom_df.loc[condition_1, '스틱비용'] = bom_df['수율_스틱'] + bom_df['loss율_스틱']

        # 2) '품명'이 '스틱-'이고,'품목자산분류'가 '원자재'일 경우
        condition_2 = (
            (bom_df['품명'].str.contains('스틱-')) &
            (bom_df['품목자산분류'] == '원자재') 
        )
        bom_df.loc[condition_2, '수율_스틱'] = 0
        bom_df.loc[condition_2, 'loss율_스틱'] = bom_df['수율_스틱'] * bom_df['loss율']
        bom_df.loc[condition_2, '스틱비용'] = bom_df['수율_스틱'] + bom_df['loss율_스틱']   
        
        # 3) '품명'이 '스틱-'이고,'품목자산분류'가 '원자재','0'이 아닐 경우
        condition_3 = (
            (bom_df['품명'].str.contains('스틱-')) &
            ~(bom_df['품목자산분류'].isin(['원자재', '0'])) 
        )
        bom_df.loc[condition_3, '수율_스틱'] = bom_df['환산비용']
        bom_df.loc[condition_3, 'loss율_스틱'] = bom_df['수율_스틱'] * bom_df['loss율']
        bom_df.loc[condition_3, '스틱비용'] = bom_df['수율_스틱'] + bom_df['loss율_스틱']    

        # 4) '품명'이 '스틱-'이고,'조달구분'이 '부재료비'일 경우
        condition_4 = (
            (bom_df['품명'].str.contains('스틱-')) &
            (bom_df['조달구분'] == '부재료비') 
        )
        bom_df.loc[condition_4, '수율_스틱'] = bom_df['단가']
        bom_df.loc[condition_4, 'loss율_스틱'] = bom_df['수율_스틱'] * bom_df['loss율']
        bom_df.loc[condition_4, '스틱비용'] = bom_df['수율_스틱'] + bom_df['loss율_스틱']   
        

        # 5) '품명'에 '스틱-'포함되어 있고 '공정흐름차수명'이 '노무비' 또는 '제조경비'일 때 동일한 '품번'마다의 '단가' 값을 '스틱비용'에 할당
        bom_df.loc[
            (bom_df['품명'].str.contains('스틱-')) & (bom_df['공정흐름차수명'].isin(['노무비', '제조경비'])),
            '스틱비용'
        ] = bom_df['단가']


        # 결과를 'BOM_스틱_원두.csv' 파일로 저장
        bom_df.to_csv(bom_file, index=False, encoding='utf-8-sig')
        print("BOM 스틱 가공 완료 - BOM_스틱.csv 파일 갱신 완료")
        logging.info("BOM 스틱 가공 작업이 완료되었습니다.")

    except FileNotFoundError as e:
        logging.error(f"파일을 찾을 수 없습니다: {e}")
        print("오류: 파일을 찾을 수 없습니다.")
    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {e}")
        print("오류 발생, 로그 파일을 확인하세요.")

if __name__ == "__main__":
    main()
