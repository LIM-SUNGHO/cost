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

        # 파일 불러오기 (encoding='utf-8-sig')
        bom_df = pd.read_csv(bom_file, encoding='utf-8-sig', low_memory=False)
        logging.info("BOM 파일을 성공적으로 불러왔습니다.")

        # 1) '자재번호'와 '품번'을 문자열 타입으로 변환하여 일관성 확보
        bom_df['자재번호'] = bom_df['자재번호'].astype(str)
        bom_df['품번'] = bom_df['품번'].astype(str)
        logging.info("'자재번호'와 '품번'을 문자열로 변환하여 일관성을 확보했습니다.")
        
        # '수율_추출' 열 생성 및 '공정'이 '추출'일 때 처리
        bom_df['수율_추출'] = np.where(
            bom_df['공정'] == '추출', 
            bom_df['추출비용'] / bom_df['수율'].replace(0, np.nan), 
            0
        )
        bom_df['수율_추출'] = bom_df['수율_추출'].fillna(0).astype(float)
        logging.info("수율_추출 열 생성 완료.")

        # 'loss율_추출' 열 생성 및 '공정'이 '추출'일 때 처리
        bom_df['loss율_추출'] = np.where(
            bom_df['공정'] == '추출', 
            bom_df['수율_추출'] * bom_df['loss율'], 
            0
        )
        bom_df['loss율_추출'] = bom_df['loss율_추출'].fillna(0).astype(float)
        logging.info("loss율_추출 열 생성 완료.")

        # 3) '추출_routing' 생성 및 값 할당
        bom_df['추출_routing'] = 0.0  # 초기값을 float으로 설정
        mask_추출_routing = (bom_df['공정'] == '추출') & (bom_df['자재명'] == '0') & (~bom_df['비고'].str.contains('외주', na=False))
        mask_전체 = mask_추출_routing & (bom_df['조달구분'] != '운반비')
        추출_routing_합산 = bom_df[mask_전체].groupby('품번')['단가'].agg('sum')
        
        for 품번, value in 추출_routing_합산.items():
            bom_df.loc[bom_df['품번'] == 품번, '추출_routing'] = value
        logging.info("추출_routing 계산 및 값 할당을 완료했습니다.")

        # 4) '추출_routing_외주' 생성 및 값 할당
        bom_df['추출_routing_외주'] = 0.0  # 초기값을 float으로 설정
        mask_추출_routing_외주 = (bom_df['공정'] == '추출') & (bom_df['자재명'] == '0') & (bom_df['비고'].str.contains('외주', na=False))

        # 추출_routing_외주 합산 계산
        for 품번 in bom_df['품번'].unique():
            품번_mask = bom_df['품번'] == 품번
            
            # '외주' 조건 확인
            외주_rows = bom_df[mask_추출_routing_외주 & 품번_mask]
            if len(외주_rows) == 1:  # '외주' 값이 1개인 경우
                추출_routing = bom_df.loc[품번_mask, '추출_routing'].iloc[0]  # 추출_routing 값
                외주_단가 = 외주_rows['단가'].iloc[0]  # 외주 단가 값
                bom_df.loc[품번_mask, '추출_routing_외주'] = 추출_routing + 외주_단가
            elif len(외주_rows) > 1:  # '외주' 값이 여러 개인 경우
                total_외주_단가 = 외주_rows['단가'].sum()
                bom_df.loc[품번_mask, '추출_routing_외주'] = total_외주_단가

        logging.info("추출_routing_외주 계산 및 조건에 따른 합산 값을 할당했습니다.")

        # 'loss율_추출'이 0일 때 '추출_routing'과 '추출_routing_외주' 값을 0으로 설정
        bom_df.loc[bom_df['loss율_추출'] == 0, ['추출_routing', '추출_routing_외주']] = 0
        logging.info("'loss율_추출'이 0인 경우 '추출_routing'과 '추출_routing_외주' 값 0으로 설정 완료.")

        # 결과 저장
        bom_df.to_csv(bom_file, index=False, encoding='utf-8-sig')
        print("BOM 추출 작업 완료 - BOM_추출_액상,추출액.csv 파일 갱신 완료")
        logging.info("BOM 추출 작업이 완료되었습니다.")

    except FileNotFoundError as e:
        logging.error(f"파일을 찾을 수 없습니다: {e}")
        print("오류: 파일을 찾을 수 없습니다.")
    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {e}")
        print("오류 발생, 로그 파일을 확인하세요.")

if __name__ == "__main__":
    main()
