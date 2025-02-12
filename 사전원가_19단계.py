import pandas as pd
import os
import logging

# 현재 스크립트 위치를 기준으로 작업 디렉토리 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

UPLOAD_DIR = "uploads"

# 로그 설정
logging.basicConfig(filename="log_BOM_사전원가_액상,추출액.log", level=logging.INFO, 
                    format="%(asctime)s - %(levelname)s - %(message)s")

def process_file(file_name):
    try:
        # 파일 불러오기
        bom_df = pd.read_csv(file_name, encoding='utf-8-sig', low_memory=False)
        logging.info(f"파일 '{file_name}'을 성공적으로 불러왔습니다.")

        # 0) '품목대분류'가 '원두'인 데이터만 추출
        bom_df = bom_df[bom_df['품목대분류'].isin(['액상','추출액'])]
        logging.info("'품목대분류'가 '액상','추출액' 인 데이터만 추출하였습니다.")

        # 1) '공정' 헤더의 값이 '스티커','재활용분담금','트레이더스' 인 경우 '자재명'으로 값을 채움
        bom_df.loc[bom_df['공정'].isin(['스티커', '재활용분담금', '트레이더스']), '자재명'] = bom_df.loc[bom_df['공정'].isin(['스티커', '재활용분담금', '트레이더스']), '공정']

        # 2) '구분' 헤더 생성 후 '공정흐름차수명' 값을 채움
        bom_df['구분'] = bom_df['공정흐름차수명']

        # 3) '구분' 값이 '0'인 경우 '포장'으로 변경
        bom_df.loc[bom_df['구분'] == '0', '구분'] = '포장'

        # 4) '공정.1' 헤더 생성 후 '공정' 값을 채움
        bom_df['공정.1'] = bom_df['공정']

        # 5) '공정.1' 값이 '스티커', '재활용분담금'인 경우 '포장'으로 변경
        bom_df.loc[bom_df['공정.1'].isin(['스티커', '재활용분담금']), '공정.1'] = '포장'

        # 6) '항목' 헤더 생성
        bom_df['항목'] = None
        조건1 = bom_df['품목자산분류'].isin(['부자재', '원자재'])
        bom_df.loc[조건1, '항목'] = bom_df.loc[조건1, '품목자산분류']

        # 7) '자재명' 값이 특정 패턴인 경우 '항목'에 '원자재'로 설정
        조건2 = bom_df['자재명'].str.contains('배전-|배/착-|분쇄-|분/착-|스틱-', na=False)
        bom_df.loc[조건2, '항목'] = '원자재'

        # 8) '자재명' 값이 '0'인 경우 '항목'에 '조달구분' 값을 설정
        bom_df.loc[bom_df['자재명'] == '0', '항목'] = bom_df.loc[bom_df['자재명'] == '0', '조달구분']

        # 9) 헤더 순서 조정
        selected_columns = [
            '품목대분류', '품번', '품명', '규격', '단위', '구분', '공정.1', '항목',
            '자재번호', '자재명', '자재규격', '단위.2', '사전원가'
        ]
        bom_df = bom_df[selected_columns]

        # 10) 헤더 이름 변경
        bom_df.rename(columns={'공정.1': '공정', '단위.2': '자재단위'}, inplace=True)

        # 11) '품목대분류' 값이 '액상'인 행을 위로 이동
        bom_df = pd.concat([
            bom_df[bom_df['품목대분류'] == '액상'],
            bom_df[~bom_df['품목대분류'].isin(['원두', '조제'])]
        ]).reset_index(drop=True)
       
        # 12) '품번'별로 '구분'이 '포장'이 아닌 경우 '공정'의 순서를 지정된 순서로 정렬
        공정_순서 = {'배전': 1, '배/착': 2, '분쇄': 3, '분/착': 4, '스틱': 5}
        bom_df['공정_순서'] = bom_df['공정'].map(공정_순서).fillna(99)
        bom_df = bom_df.sort_values(by=['품번', '구분', '공정_순서']).drop(columns=['공정_순서'])

        # 13) '품번'별로 행의 순서를 조정 - '구분' 값에서 '포장'이 제일 위로 배치
        bom_df = bom_df.assign(포장_우선순위=bom_df['구분'].eq('포장').astype(int))
        bom_df = bom_df.sort_values(by=['품번', '포장_우선순위'], ascending=[True, False]).drop(columns=['포장_우선순위'])

        # 14) '품번'별로 '구분'이 '포장'인 경우 '항목'이 '원자재', '부자재' 순서로 배치
        항목_순서 = {'원자재': 1, '부자재': 2, '포장': 0}
        bom_df['항목_순서'] = bom_df['항목'].map(항목_순서).fillna(99)
        bom_df = bom_df.sort_values(by=['품번', '항목_순서']).drop(columns=['항목_순서'])

        # 15) 값이 0인 경우 빈칸으로 처리
        bom_df = bom_df.replace('0', '').replace(0, '')

        # 중복값 제거 (첫 번째 값만 유지)
        bom_df = bom_df.drop_duplicates()

        # 데이터 분리 및 저장
        액상_df = bom_df[bom_df['품목대분류'] == '액상']
        추출액_df = bom_df[bom_df['품목대분류'] == '추출액']

        액상_df.to_csv(os.path.join(UPLOAD_DIR,"사전원가_액상.csv"), index=False, encoding='utf-8-sig')
        logging.info("파일 저장 완료: '사전원가_액상.csv'")
        print("파일 저장 완료: '사전원가_액상.csv'")

        추출액_df.to_csv(os.path.join(UPLOAD_DIR,"사전원가_추출액.csv"), index=False, encoding='utf-8-sig')
        logging.info("파일 저장 완료: '사전원가_추출액.csv'")
        print("파일 저장 완료: '사전원가_추출액.csv'")

    except Exception as e:
        logging.error(f"파일 '{file_name}' 처리 중 오류 발생: {e}")
        print(f"파일 '{file_name}' 처리 중 오류 발생: {e}")
        
        
def main():
    try:
        # 고정된 입력 파일명 설정
        input_file = os.path.join(UPLOAD_DIR,"사전원가_액상,추출액.csv")

        if not os.path.exists(input_file):
            print(f"'{input_file}' 파일이 존재하지 않습니다.")
            logging.info(f"'{input_file}' 파일이 존재하지 않습니다.")
            return

        process_file(input_file)
        logging.info("모든 파일 처리 완료.")
    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {e}")
        print(f"오류 발생: {e}")
        print("자세한 내용은 로그 파일을 확인하세요.")

if __name__ == "__main__":
    main()

