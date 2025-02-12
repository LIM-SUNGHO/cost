import pandas as pd
import os
import logging

UPLOAD_DIR = "uploads"

# 현재 스크립트 위치를 기준으로 작업 디렉토리 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# 로그 설정
logging.basicConfig(filename="log_BOM_가공.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    try:
        # 파일 경로 설정
        bom_file = os.path.join(UPLOAD_DIR,'BOM_가공.csv')
        cost_file = os.path.join(UPLOAD_DIR,'원부재료 사전원가.csv')

        # 파일 불러오기 (encoding='utf-8-sig')
        bom_df = pd.read_csv(bom_file, encoding='utf-8-sig',low_memory=False)
        cost_df = pd.read_csv(cost_file, encoding='utf-8-sig',low_memory=False)

        # 2-1. '단가' 헤더 생성 및 매칭된 '원가' 값 추출
        if '단가' not in bom_df.columns:
            bom_df['단가'] = 0  # 기본값을 0으로 설정

        bom_df['단가'] = bom_df.apply(
            lambda row: cost_df[cost_df['품번'] == row['자재번호']]['원가'].values[0] if not cost_df[cost_df['품번'] == row['자재번호']].empty else row['단가'],
            axis=1
        )

        # 2-2. '환산비용' 헤더 생성 및 계산 (단가 * BOM환산수량)
        bom_df['환산비용'] = bom_df['단가'] * bom_df['BOM환산수량']

        logging.info("단가 및 환산비용 계산이 완료되었습니다.")

        # 3. 결과를 'BOM_가공.csv' 파일로 저장
        bom_df.to_csv(bom_file, index=False, encoding='utf-8-sig')
        print("BOM 원가 매칭 완료 - BOM_가공.csv 파일 생성 완료")
        logging.info("BOM 원가 매칭 작업이 완료되었습니다.")

    except FileNotFoundError as e:
        logging.error(f"파일을 찾을 수 없습니다: {e}")
        print("오류: 파일을 찾을 수 없습니다.")
    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {e}")
        print("오류 발생, 로그 파일을 확인하세요.")

if __name__ == "__main__":
    main()
