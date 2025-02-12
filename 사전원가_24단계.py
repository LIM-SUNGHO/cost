import pandas as pd
import os
import logging

# 현재 스크립트 위치를 기준으로 작업 디렉토리 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

UPLOAD_DIR = "uploads"

# 로그 설정
logging.basicConfig(filename="log_BOM_배전_원두.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    try:
        # 파일 경로 설정
        bom_file = os.path.join(UPLOAD_DIR,'BOM_배전_원두.csv')

        # 파일 불러오기 (encoding='utf-8-sig')
        bom_df = pd.read_csv(bom_file, encoding='utf-8-sig', low_memory=False)
        logging.info("BOM 파일을 성공적으로 불러왔습니다.")

        # 데이터 타입 일치
        bom_df['자재번호'] = bom_df['자재번호'].astype(str)
        
        # 배전 비용 합산
        items = bom_df.copy()
        items['품번'] = items['품번'].astype(str)
        cost_sum = items.groupby('품번')['배전비용'].sum().reset_index()
        logging.info("배전비용 합산 작업 완료.")

        # merge 작업
        bom_df = bom_df.merge(cost_sum, 
                              left_on='자재번호', 
                              right_on='품번', 
                              how='left', 
                              suffixes=('_x', '_y'))
        logging.info("데이터 병합 작업 완료.")

        # 단가_배전 설정
        if '배전비용_y' in bom_df.columns:
            bom_df['단가_배전'] = bom_df['배전비용_y'].fillna(0)
        else:
            logging.warning("배전비용 열을 찾을 수 없습니다.")
            print("현재 컬럼:", bom_df.columns.tolist())

        # '배전비용'이 0이고 '단가_배전'이 0이 아닌 경우, '배전비용'에 '단가_배전' 값을 넣기
        bom_df.loc[(bom_df['단가_배전'] == 0) & (bom_df['배전비용_x'] != 0), '단가_배전'] = bom_df['배전비용_x']
        logging.info("단가_배전이 0이고 배전비용이 0이 아닌 경우에 배전비용 값을 업데이트 완료.")

        # 열 이름 변경 및 불필요한 열 삭제
        bom_df = bom_df.rename(columns={'품번_x': '품번'})
        bom_df = bom_df.drop(columns=['배전비용_y', '품번_y','배전비용_x'], errors='ignore')
        logging.info("열 이름 변경 및 불필요한 열 삭제 완료.")

        # '공정'이 '추출'인 값에서 '품번', '자재번호', '단가','비고','조달구분' 중복값을 제거
        bom_df = bom_df[~((bom_df['공정'] == '추출') & 
                          bom_df.duplicated(subset=['품번', '자재번호', '단가', '비고', '조달구분']))]
        logging.info("공정이 '추출'인 값에서 '품번', '자재번호', '단가', '비고', '조달구분' 중복값 제거 완료.")

        # 결과 저장
        bom_df.to_csv(bom_file, index=False, encoding='utf-8-sig')
        print("BOM 배전 가공 완료 - BOM_배전_원두.csv 파일 갱신 완료")
        logging.info("BOM 배전 가공 작업이 완료되었습니다.")

    except FileNotFoundError as e:
        logging.error(f"파일을 찾을 수 없습니다: {e}")
        print("오류: 파일을 찾을 수 없습니다.")
    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {e}")
        print("오류 발생, 로그 파일을 확인하세요.")

if __name__ == "__main__":
    main()
