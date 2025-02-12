import pandas as pd
import os
import logging

# 현재 스크립트 위치를 기준으로 작업 디렉토리 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

UPLOAD_DIR = "uploads"

# 로그 설정
logging.basicConfig(filename="log_BOM_미세_조제.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    try:
        # 파일 경로 설정
        bom_file = os.path.join(UPLOAD_DIR,'BOM_미세_조제.csv')

        # 파일 불러오기 (encoding='utf-8-sig')
        bom_df = pd.read_csv(bom_file, encoding='utf-8-sig', low_memory=False)
        logging.info("BOM 파일을 성공적으로 불러왔습니다.")

        # '단가_배전_y'과 '미세단가'의 합으로 '단가_미세' 열 생성
        if '단가_배전_y' in bom_df.columns and '미세단가' in bom_df.columns:
            bom_df['단가_미세'] = bom_df['단가_배전_y'].fillna(0) + bom_df['미세단가'].fillna(0)
            logging.info("'단가_미세' 열 생성 완료.")
        else:
            missing_columns = [col for col in ['단가_배전_y', '미세단가'] if col not in bom_df.columns]
            logging.warning(f"필요한 열이 없습니다: {missing_columns}")
            print(f"오류: 필요한 열이 없습니다: {missing_columns}")
            return

        # 추가 조건: '품목대분류'가 '반제품'이고, '품명'에 '미세'이 포함되며, '공정흐름차수명'에 '노무비' 또는 '제조경비'가 있는 경우
        if '품목대분류' in bom_df.columns and '품명' in bom_df.columns and '공정흐름차수명' in bom_df.columns and '단가' in bom_df.columns:
            bom_df['단가_미세'] = bom_df.apply(
                lambda row: row['단가'] if row['품목대분류'] == '반제품' and '미세' in str(row['품명']) and 
                (row['공정흐름차수명'] in ['노무비', '제조경비']) else row['단가_미세'],
                axis=1
            )
            logging.info("특정 조건에 따라 '단가_미세' 값을 '단가'로 채웠습니다.")
        else:
            missing_columns = [col for col in ['품목대분류', '품명', '공정흐름차수명', '단가'] if col not in bom_df.columns]
            logging.warning(f"필요한 열이 없습니다: {missing_columns}")
            print(f"오류: 필요한 열이 없습니다: {missing_columns}")


        # '미세비용'이 0이고 '단가_배전'이 0이 아닌 경우, '미세비용'에 '단가_배전' 값을 넣기
        bom_df.loc[(bom_df['단가_미세'] == 0) & (bom_df['미세비용'] != 0), '단가_미세'] = bom_df['미세비용']
        logging.info("미세_미세이 0이고 미세비용이 0이 아닌 경우에 미세비용 값을 업데이트 완료.")

        # '단가_배전_y'와 '미세단가' 열 제거
        bom_df = bom_df.drop(columns=['단가_배전_y', '미세단가','미세비용'], errors='ignore')
        logging.info("'단가_배전_y'와 '미세단가','미세비용' 열 삭제 완료.")
        # 열 이름 변경
        bom_df = bom_df.rename(columns={'단가_배전_x': '단가_배전'})

        # '자재명' 열에 특정 키워드가 포함된 경우 '단가_미세' 값을 0으로 설정
        if '자재명' in bom_df.columns and '단가_미세' in bom_df.columns:
            keywords = ['배/착', '분쇄', '추출-', '조/배-']
            for keyword in keywords:
                bom_df.loc[bom_df['자재명'].str.contains(keyword, na=False), '단가_미세'] = 0
                logging.info(f"'자재명'에 '{keyword}'이 포함된 행에 대해 '단가_미세' 값을 0으로 설정했습니다.")
        else:
            logging.warning("'자재명' 또는 '단가_미세' 열이 없습니다. 해당 작업을 건너뜁니다.")

        
        # '품목대분류'가 '조제'인 데이터에서 중복 제거
        # 중복 기준: 해당 행의 모든 값
        filtered_df = bom_df[bom_df['품목대분류'] == '조제']
        non_duplicates = filtered_df.drop_duplicates(keep='first')
        
        # '조제' 아닌 데이터 유지 및 합치기
        bom_df = pd.concat([bom_df[bom_df['품목대분류'] != '조제'], non_duplicates], ignore_index=True)

        # 결과 저장
        bom_df.to_csv(bom_file, index=False, encoding='utf-8-sig')
        print("BOM 미세 가공 완료 - BOM_미세.csv 파일 갱신 완료")
        logging.info("BOM 미세 가공 작업이 완료되었습니다.")

    except FileNotFoundError as e:
        logging.error(f"파일을 찾을 수 없습니다: {e}")
        print("오류: 파일을 찾을 수 없습니다.")
    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {e}")
        print("오류 발생, 로그 파일을 확인하세요.")

if __name__ == "__main__":
    main()
