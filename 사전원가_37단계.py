import pandas as pd
import os
import re
import logging

# 현재 스크립트 위치를 기준으로 작업 디렉토리 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

UPLOAD_DIR = "uploads"

# 로그 설정
logging.basicConfig(
    filename="log_BOM_사전원가_원두.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():
    try:
        # 파일 경로 설정
        input_file = os.path.join(UPLOAD_DIR, 'BOM_스틱_원두.csv')

        # 파일 불러오기 (encoding='utf-8-sig')
        df = pd.read_csv(input_file, encoding='utf-8-sig',low_memory=False)
        logging.info("BOM 파일을 성공적으로 불러왔습니다.")

        # '입수' 헤더 생성 및 값 추가
        def extract_number(value):
            """'규격' 값에서 'p' 또는 'P' 앞, '*' 뒤의 숫자 추출"""
            if pd.isna(value):
                return None
            try:
                # 'p' 또는 'P' 앞의 값 추출
                if 'p' in value.lower():
                    value = value.split('p')[0].strip()
                # '*' 뒤의 숫자 추출
                match = re.search(r'\*(\d+)', value)
                return int(match.group(1)) if match else None
            except ValueError:
                logging.warning(f"값 변환 오류: {value}")
                return None

        df['입수'] = df['규격'].apply(extract_number)

        # '품명'에 '시그니처 팩'이 포함된 경우 '입수' 값을 1로 설정
        signature_condition = df['품명'].str.contains('시그니처 팩', na=False)
        df.loc[signature_condition, '입수'] = 1
        logging.info("'품명'에 '시그니처 팩' 값이 포함된 경우 '입수'를 1로 설정했습니다.")

        # 처리 결과 저장
        output_file = os.path.join(UPLOAD_DIR,'사전원가_원두.csv')
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logging.info("가공된 데이터가 기존 데이터에 병합되었습니다.")
        print("BOM 추출 가공 완료 - 데이터 병합 후 원본 파일 업데이트 완료")

    except FileNotFoundError as e:
        logging.error(f"파일을 찾을 수 없습니다: {e}")
        print("오류: 파일을 찾을 수 없습니다.")
    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {e}")
        print("오류 발생, 로그 파일을 확인하세요.")

if __name__ == "__main__":
    main()
