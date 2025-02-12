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

        # 1) '자재번호'와 '품번'을 문자열 타입으로 변환하여 일관성 확보
        bom_df['자재번호'] = bom_df['자재번호'].astype(str)
        bom_df['품번'] = bom_df['품번'].astype(str)
        logging.info("'자재번호'와 '품번'을 문자열로 변환하여 일관성을 확보했습니다.")

        # 2) '추출량비율'이 '0', '1'이 아닐 때 조건에 해당하는 '품번' 추출
        조건_추출량비율 = (bom_df['추출량비율'] != 0) & (bom_df['추출량비율'] != 1)
        중복_품번 = bom_df[조건_추출량비율].groupby('품번').filter(
            lambda x: len(x['자재번호'].unique()) > 1 and len(x['추출비용'].unique()) > 1
        )['품번'].unique()
        logging.info(f"추출된 품번 수: {len(중복_품번)}개")
        
        # 3) '품번'별로 ('수율_추출' + 'loss율_추출' + '추출_routing') * '추출량비율' 계산 및 집계
        계산_천안 = bom_df[bom_df['품번'].isin(중복_품번)].copy()
        계산_천안['계산값_천안'] = 계산_천안['수율_추출'] + 계산_천안['loss율_추출'] + 계산_천안['추출_routing'] - (계산_천안['추출_routing']/2)
        계산_천안_집계 = 계산_천안.groupby('품번', as_index=False)['계산값_천안'].sum()


        # 4) '품번'별로 ('수율_추출' + 'loss율_추출' + '추출_routing_외주') * '추출량비율' 계산 및 집계
        계산_외주 = bom_df[bom_df['품번'].isin(중복_품번)].copy()
        계산_외주['계산값_외주'] = 계산_외주['수율_추출'] + 계산_외주['loss율_추출'] + 계산_외주['추출_routing_외주'] - (계산_외주['추출_routing_외주']/2)
        계산_외주_집계 = 계산_외주.groupby('품번', as_index=False)['계산값_외주'].sum()


        # 데이터 타입 통일
        중복_품번 = 중복_품번.astype(str)  # 중복_품번을 문자열로 변환
        bom_df['품번'] = bom_df['품번'].astype(str).str.strip()  # 공백 제거 및 문자열 변환
        bom_df['자재번호'] = bom_df['자재번호'].astype(str).str.strip()  # 공백 제거 및 문자열 변환


        # 5) '단가_추출_천안' 값 업데이트
        천안_공정 = ['미세', '배/착', '배전', '배합', '분/착', '분쇄', '제/배', '포장']
        if '단가_추출_천안' not in bom_df.columns:
            bom_df['단가_추출_천안'] = np.nan  # 열이 없으면 생성

        천안_값_매핑 = 계산_천안_집계.set_index('품번')['계산값_천안']

        # mask_천안 조건 디버깅
        mask_천안 = (
            (bom_df['자재번호'].isin(중복_품번)) &  # 자재번호가 중복_품번에 포함
            (bom_df['공정'].isin(천안_공정))       # 공정이 천안_공정 리스트에 포함
        )

        # 매핑 로직 수정 및 값 업데이트
        mapped_values = bom_df.loc[mask_천안, '자재번호'].map(천안_값_매핑)
        bom_df.loc[mask_천안, '단가_추출_천안'] = mapped_values.fillna(bom_df.loc[mask_천안, '단가_추출_천안'])

        # 6) '단가_추출_외주' 값 업데이트
        if '단가_추출_외주' not in bom_df.columns:
            bom_df['단가_추출_외주'] = np.nan  # 열이 없으면 생성

        외주_값_매핑 = 계산_외주_집계.set_index('품번')['계산값_외주']


        # mask_외주 조건 디버깅
        mask_외주 = (
            (bom_df['자재번호'].isin(중복_품번)) &  # 자재번호가 중복_품번에 포함
            (~bom_df['공정'].isin(천안_공정))      # 공정이 천안_공정 리스트에 포함되지 않음
        )

        # 매핑 로직 수정 및 값 업데이트
        mapped_values_외주 = bom_df.loc[mask_외주, '자재번호'].map(외주_값_매핑)
        bom_df.loc[mask_외주, '단가_추출_외주'] = mapped_values_외주.fillna(bom_df.loc[mask_외주, '단가_추출_외주'])

        # '공정'이 '추출'인 값에서 '품번', '자재번호', '단가','비고','조달구분' 중복값을 제거
        bom_df = bom_df[~(bom_df.duplicated(subset=['품번', '자재번호', '단가', '비고', '조달구분','환산비용','단가_추출_천안','단가_추출_외주']))]
        logging.info("공정이 '추출'인 값에서 '품번', '자재번호', '단가', '비고', '조달구분','환산비용','단가_추출_천안','단가_추출_외주' 중복값 제거 완료.")

        # 결과 저장
        bom_df.to_csv(bom_file, index=False, encoding='utf-8-sig')
        print("\nBOM 추출 작업 완료 - BOM_추출_액상,추출액.csv 파일 갱신 완료")
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
