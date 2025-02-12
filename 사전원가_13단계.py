import pandas as pd
import os
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
        bom_file = os.path.join(UPLOAD_DIR,'BOM_분쇄_액상,추출액.csv')

        # 파일 불러오기 (encoding='utf-8-sig')
        bom_df = pd.read_csv(bom_file, encoding='utf-8-sig', low_memory=False)
        logging.info("BOM 파일을 성공적으로 불러왔습니다.")

        # 자재명 열의 데이터에서 공백 제거
        bom_df['자재명'] = bom_df['자재명'].astype(str).str.strip()

        # '공정'이 '추출' 또는 '품목대분류'가 '추출액'이고 '자재명'에 '분쇄'가 포함된 경우 필터링 후 '소요량분자' 값 합산
        filtered_df = bom_df[((bom_df['공정'] == '추출') | (bom_df['품목대분류'] == '추출액')) & 
                            (bom_df['자재명'].str.contains('분쇄', na=False, regex=False))]

        if filtered_df.empty:
            logging.warning("필터링된 데이터가 없습니다. '공정'이 '추출' 또는 '품목대분류'가 '추출액'이고 '자재명'에 '분쇄'가 포함된 데이터가 없습니다.")
        else:
            logging.info(f"필터링된 데이터 건수: {len(filtered_df)}")

        # '품번'별로 필터링된 '소요량분자' 합산하여 '소요량합산' 생성
        sum_by_item = filtered_df.groupby('품번')['소요량분자'].sum().reset_index()
        sum_by_item = sum_by_item.rename(columns={'소요량분자': '소요량합산'})
        logging.info("'소요량합산' 생성 완료 - '품번'별로 '소요량분자' 합산.")

        # 원본 데이터프레임에 '소요량합산' 병합
        bom_df = bom_df.merge(sum_by_item, on='품번', how='left')

        # '추출량비율' 열 생성 및 데이터 유형을 float로 설정하여 필터링된 행에만 값 계산, 나머지는 0으로 설정
        bom_df['추출량비율'] = 0.0  # 기본값을 0.0으로 설정 (float 유형)
        bom_df.loc[filtered_df.index, '추출량비율'] = bom_df.loc[filtered_df.index, '소요량분자'] / bom_df.loc[filtered_df.index, '소요량합산']
        bom_df['추출량비율'] = bom_df['추출량비율'].fillna(0)  # 결측값을 0으로 대체
        logging.info("'추출량비율' 열 생성 및 계산 완료.")

        # 불필요한 '소요량합산' 열 삭제
        bom_df = bom_df.drop(columns=['소요량합산'], errors='ignore')
        logging.info("'소요량합산' 열 삭제 완료.")

        # '공정'이 '추출'인 값에서 '품번', '자재번호', '단가'의 중복 제거
        bom_df = bom_df[~((bom_df['공정'] == '추출') & 
                          bom_df.duplicated(subset=['품번', '자재번호', '단가','비고','조달구분']))]
        logging.info("공정이 '추출'인 값에서 '품번', '자재번호', '단가','비고','조달구분' 중복값을 제거했습니다.")

        # 결과 저장
        output_file = os.path.join(UPLOAD_DIR,'BOM_추출_액상,추출액.csv')
        bom_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print("BOM 원가 가공 완료 - BOM_가공.csv 파일 갱신 완료")
        logging.info("BOM 원가 가공 작업이 완료되었습니다.")

    except FileNotFoundError as e:
        logging.error(f"파일을 찾을 수 없습니다: {e}")
        print("오류: 파일을 찾을 수 없습니다.")
    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {e}")
        print("오류 발생, 로그 파일을 확인하세요.")

if __name__ == "__main__":
    main()
