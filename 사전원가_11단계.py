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
        logging.info("BOM 파일을 성공적으로 불러왔습니다.")

        # 데이터 타입 일관성 확보
        bom_df['자재번호'] = bom_df['자재번호'].astype(str)

        # '품번'별 '단가_배전'과 '분쇄비용'을 합산하여 병합 준비
        items = bom_df.copy()
        items['품번'] = items['품번'].astype(str)

        # 단가_배전 합산
        dan_ga_sum = items.groupby('품번')['단가_배전'].sum().reset_index()
        logging.info("단가_배전 합산 작업 완료.")

        # 분쇄비용 합산
        bun_sae_cost_sum = items.groupby('품번')['분쇄비용'].sum().reset_index()
        logging.info("분쇄비용 합산 작업 완료.")

        # '자재번호'와 '품번'을 기준으로 병합하여 '분쇄비용' 정보 추가
        bom_df = bom_df.merge(dan_ga_sum, left_on='자재번호', right_on='품번', how='left', suffixes=('_x', '_y'))
        bom_df = bom_df.merge(bun_sae_cost_sum, left_on='자재번호', right_on='품번', how='left', suffixes=('', '_분쇄'))

        # 분쇄단가 설정
        if '분쇄비용_분쇄' in bom_df.columns:
            bom_df['분쇄단가'] = bom_df['분쇄비용_분쇄'].fillna(0)
        else:
            logging.warning("분쇄비용 열을 찾을 수 없습니다.")
            print("현재 컬럼:", bom_df.columns.tolist())

        # 열 이름 변경 및 불필요한 열 삭제
        bom_df = bom_df.drop(columns=['분쇄비용_분쇄', '품번_y', '품번'], errors='ignore')
        bom_df = bom_df.rename(columns={'품번_x': '품번','분쇄비용_x': '분쇄비용'})
        logging.info("열 이름 변경 및 불필요한 열 삭제 완료.")

        # 결과 저장
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
