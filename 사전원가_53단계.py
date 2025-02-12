import pandas as pd
import os
import logging

# 현재 스크립트 위치를 기준으로 작업 디렉토리 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

UPLOAD_DIR = "uploads"

# 로그 설정
logging.basicConfig(filename="log_BOM_스틱_조제.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    try:
        # 파일 경로 설정
        bom_file = os.path.join(UPLOAD_DIR,'BOM_스틱_조제.csv')

        # 파일 불러오기 (encoding='utf-8-sig')
        bom_df = pd.read_csv(bom_file, encoding='utf-8-sig', low_memory=False)
        logging.info("BOM 파일을 성공적으로 불러왔습니다.")

        # '스틱단가'값으로 '단가_스틱' 열 생성
        if '단가_미세_y' in bom_df.columns and '스틱단가' in bom_df.columns:
            bom_df['단가_스틱'] = bom_df['스틱단가'].fillna(0)
            logging.info("'단가_스틱' 열 생성 완료.")
        else:
            missing_columns = [col for col in ['단가_미세_y', '스틱단가'] if col not in bom_df.columns]
            logging.warning(f"필요한 열이 없습니다: {missing_columns}")
            print(f"오류: 필요한 열이 없습니다: {missing_columns}")
            return
        
        # '스틱단가'이 0이 아니고 '수율'이 0이 아닌 경우, '단가_분착_x'에 '단가_미세_y' 값을 넣기
        bom_df.loc[(bom_df['스틱단가'] != 0) & (bom_df['수율'] != 0), '단가_미세_x'] = bom_df['단가_미세_y']
        logging.info("스틱_스틱이 0이고 스틱비용이 0이 아닌 경우에 스틱비용 값을 업데이트 완료.")

        # '단가_미세_y'와 '스틱단가' 열 제거
        bom_df = bom_df.drop(columns=['단가_미세_y', '스틱단가','스틱비용'], errors='ignore')
        logging.info("'단가_미세_y'와 '스틱단가','스틱비용' 열 삭제 완료.")
        # 열 이름 변경
        bom_df = bom_df.rename(columns={'단가_미세_x': '단가_미세'})

        # '자재명'에 특정 키워드가 포함된 경우 '단가_스틱' 값을 0으로 설정
        keywords = ['배전', '분쇄', '추출-']
        if '자재명' in bom_df.columns and '단가_스틱' in bom_df.columns:
            for keyword in keywords:
                bom_df.loc[bom_df['자재명'].str.contains(keyword, na=False), '단가_스틱'] = 0
                logging.info(f"'자재명'에 '{keyword}'이 포함된 행에 대해 '단가_스틱' 값을 0으로 설정했습니다.")
        else:
            logging.warning("'자재명' 또는 '단가_스틱' 열이 없습니다. 해당 작업을 건너뜁니다.")
        
        # '품목대분류'가 '조제'인 데이터에서 중복 제거
        # 중복 기준: 해당 행의 모든 값
        filtered_df = bom_df[bom_df['품목대분류'] == '조제']
        non_duplicates = filtered_df.drop_duplicates(keep='first')
        
        # '조제' 아닌 데이터 유지 및 합치기
        bom_df = pd.concat([bom_df[bom_df['품목대분류'] != '조제'], non_duplicates], ignore_index=True)

        # 결과 저장
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
