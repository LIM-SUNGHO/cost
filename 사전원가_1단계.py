import pandas as pd
import logging
import os

UPLOAD_DIR = "uploads"

# 현재 스크립트 위치를 기준으로 작업 디렉토리 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# 로그 설정
logging.basicConfig(filename="log_BOM_가공.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    try:
        # 파일 경로 설정 및 파일 목록 정의
        bom_file = os.path.join(UPLOAD_DIR,'BOM.csv')
        bom_files_to_merge = [
            os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_001.csv'),
            os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_002.csv'),
            os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_003.csv'),
            os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_004.csv'),
            os.path.join(UPLOAD_DIR,'최종차수_제작_BOM_005.csv')
        ]

        # BOM.csv 파일 불러오기
        bom_df = pd.read_csv(bom_file, encoding='utf-8-sig')
        
        # 최종차수_제작_BOM 파일들을 순차적으로 불러와서 BOM.csv에 추가
        for file_name in bom_files_to_merge:
            temp_df = pd.read_csv(file_name, encoding='utf-8-sig')
            
            # 데이터 병합
            bom_df = pd.concat([bom_df, temp_df], ignore_index=True)
            logging.info(f"{file_name} 파일이 BOM.csv에 병합되었습니다.")

        # 중복 데이터 제거 ('자재번호'와 '품번'이 모두 중복되는 경우 제거)
        before_dedup = len(bom_df)
        bom_df.drop_duplicates(subset=['자재번호', '품번'], inplace=True)
        after_dedup = len(bom_df)
        logging.info(f"중복 제거 완료 - 제거된 행 수: {before_dedup - after_dedup}")

        # 데이터 저장
        output_file = os.path.join(UPLOAD_DIR,'BOM_가공.csv')
        bom_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print("BOM 가공 완료 - BOM.csv 파일에 병합 및 중복 제거 완료")

    except FileNotFoundError as e:
        logging.error(f"파일을 찾을 수 없습니다: {e}")
        print("오류: 파일을 찾을 수 없습니다.")
    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {e}")
        print("오류 발생, 로그 파일을 확인하세요.")

if __name__ == "__main__":
    main()
