import os
import pandas as pd
import logging

# 현재 스크립트 위치를 기준으로 작업 디렉토리 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

UPLOAD_DIR = "uploads"
RESULT_DIR = "results"

# 로그 설정
logging.basicConfig(filename="log_사전원가통합.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def integrate_cost_files():
    try:
        # 파일 목록 정의
        file_names = ["사전원가_액상.csv", "사전원가_원두.csv", "사전원가_조제.csv", "사전원가_추출액.csv"]

        # 데이터프레임 리스트 생성
        combined_data = []
        for file_name in file_names:
            file_path = os.path.join(UPLOAD_DIR, file_name)  # 경로를 업로드 디렉토리로 변경
            if os.path.exists(file_path):
                logging.info(f"파일 읽는 중: {file_name}")
                df = pd.read_csv(file_path, encoding='utf-8-sig')
                combined_data.append(df)
            else:
                logging.warning(f"파일을 찾을 수 없음: {file_name}")

        # 통합된 데이터프레임 생성
        if combined_data:
            combined_df = pd.concat(combined_data, ignore_index=True)
            logging.info(f"모든 파일을 통합 완료. 총 데이터 수: {len(combined_df)}")

            # '비고' 헤더를 맨 앞으로 이동
            columns = ['비고'] + [col for col in combined_df.columns if col != '비고']
            combined_df = combined_df[columns]

            # 결과 저장
            output_file = os.path.join(RESULT_DIR,'사전원가.xlsx')
            combined_df.to_excel(output_file, index=False)
            print(f"통합된 데이터가 '{output_file}' 파일로 저장되었습니다.")
            logging.info(f"통합된 데이터를 '{output_file}' 파일로 저장 완료.")
        else:
            print("통합할 데이터가 없습니다. 파일 목록을 확인해주세요.")
            logging.warning("통합할 데이터가 없습니다.")

    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {e}")
        print("오류 발생: 로그 파일을 확인하세요.")

if __name__ == "__main__":
    integrate_cost_files()