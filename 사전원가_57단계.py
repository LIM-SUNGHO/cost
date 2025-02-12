import pandas as pd
import os
import numpy as np
import logging

# 현재 스크립트 위치를 기준으로 작업 디렉토리 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

UPLOAD_DIR = "uploads"

# 로그 설정
logging.basicConfig(filename="log_BOM_사전원가_조제.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def process_file(file_name):
    try:
        # 파일 불러오기
        bom_df = pd.read_csv(file_name, encoding='utf-8-sig', low_memory=False)
        logging.info(f"파일 '{file_name}'을 성공적으로 불러왔습니다.")

        # 1) '자재번호'와 '품번'을 문자열 타입으로 변환하여 일관성 확보
        bom_df['자재번호'] = bom_df['자재번호'].astype(str).str.strip()
        bom_df['품번'] = bom_df['품번'].astype(str).str.strip()

        # 2) '배합원가' 헤더 생성 및 '조달구분'이 '구매'이고, '5xAxxxxx' 형태를 제외한 값 처리
        if '배합원가' not in bom_df.columns:
            bom_df['배합원가'] = np.nan  # '배합원가' 헤더 생성
        
        # 정규식으로 '5xAxxxxx', '5xBxxxxx' 패턴 확인 및 조건 설정
        구매_조건 = (bom_df['조달구분'] == '구매') & ~bom_df['자재번호'].str.match(r'5\d[A|B]\d{5}')

        # '배합원가'에 값 채우기
        bom_df.loc[구매_조건, '배합원가'] = bom_df.loc[구매_조건, '환산비용']

      
        # 4) '공정'이 '재활용분담금'인 경우 처리
        재활용_조건 = (bom_df['공정'].isin(['재활용분담금','동판']))
        bom_df.loc[재활용_조건, '배합원가'] = bom_df.loc[재활용_조건, '단가']

        # 5) '공정흐름차수명'이 '노무비', '제조경비', '임가공비'인 경우 처리
        흐름_조건 = bom_df['공정흐름차수명'].isin(['노무비', '제조경비', '임가공비','재료비'])
        bom_df.loc[흐름_조건, '배합원가'] = bom_df.loc[흐름_조건, '단가']

        # 'loss율_포장' 헤더 생성 후 조건에 따른 값 계산 및 추가
        if 'loss율_포장' not in bom_df.columns:
            bom_df['loss율_포장'] = np.nan  # 'loss율_포장' 헤더 생성

        # 6) '공정'이 '재활용분담금'이 아니고 '공정흐름차수명'이 특정 값이 아닌 경우
        조건1 = (
            (bom_df['공정'] != '재활용분담금') & 
            (~bom_df['공정흐름차수명'].isin(['노무비', '제조경비', '임가공비']))
        )
        bom_df.loc[조건1, 'loss율_포장'] = bom_df.loc[조건1, '배합원가'] * bom_df.loc[조건1, 'loss율']

        # 7) '공정'이 '재활용분담금'인 경우
        조건2 = (
            (bom_df['공정'] == '재활용분담금') & 
            (bom_df['자재번호'] == '0'))
        
        bom_df.loc[조건2, 'loss율_포장'] = bom_df.loc[조건2, '배합원가'] * bom_df.loc[조건2, 'loss율']

        # 8) '공정'이 '포장'이고 '공정흐름차수명'이 '노무비', '제조경비', '임가공비'인 경우
        조건3 = (
            (bom_df['공정'] == '포장') & 
            (bom_df['공정흐름차수명'].isin(['노무비', '제조경비', '임가공비']))
        )
        bom_df.loc[조건3, 'loss율_포장'] = 0

        # 9) 조건 설정: '조달구분'이 '스트로우'인 경우
        조건4 = bom_df['조달구분'] == '스트로우'
        # '배합원가' * 'loss율' 값을 'loss율_포장'에 추가
        bom_df.loc[조건4, 'loss율_포장'] = bom_df.loc[조건4, '배합원가'] * bom_df.loc[조건4, 'loss율']

        # 10) '공정흐름차수명'이 '재료비'고 '비고'가 특정 값인 경우
        조건5 = ((bom_df['공정흐름차수명'] == '재료비') & (bom_df['비고'].isin(['삼양','동원시스템즈'])))
        bom_df.loc[조건5, 'loss율_포장'] = 0

        # 11) '조달구분'이 '동판'인 경우
        조건6 = bom_df['조달구분'] == '동판'
        bom_df.loc[조건6, 'loss율_포장'] = bom_df.loc[조건6, '배합원가'] * bom_df.loc[조건6, 'loss율']

        # 12) '사전원가' 헤더 생성 후 '배합원가' + 'loss율_포장' 계산
        if '사전원가' not in bom_df.columns:
            bom_df['사전원가'] = np.nan  # '사전원가' 헤더 생성

        # '배합원가'와 'loss율_포장' 계산
        bom_df['사전원가'] = bom_df['배합원가'] + bom_df['loss율_포장']

        # 16) '품목대분류'가 '조제'인 경우
        조건10 = (bom_df['품목대분류'] == '조제')
        
        # '사전원가' 계산
        bom_df.loc[조건10, '사전원가'] = bom_df.loc[조건10, '사전원가_조제']

        # '배합원가'에 '조제' 값 저장
        bom_df.loc[조건10, '배합원가'] = bom_df.loc[조건10, '조제']

        # 'loss율_포장'에 '조제_loss' 값 저장
        bom_df.loc[조건10, 'loss율_포장'] = bom_df.loc[조건10, '조제_loss']

        # '조달구분' 값을 '품목자산분류'에 추가
        if '품목자산분류' not in bom_df.columns:
            bom_df['품목자산분류'] = pd.NA  # '품목자산분류' 열 생성

        # '조달구분' 값을 '품목자산분류'에 복사
        target_values = [
            '가공비', '간접비', '감가상각비', '동판', '로스팅', '몰드비', '복리후생비',
            '부재료비', '상차비', '수도광열비', '스트로우', '운반비', '운임비', 
            '이산화탄소', '전력비'
        ]
        bom_df.loc[bom_df['조달구분'].isin(target_values), '품목자산분류'] = bom_df['조달구분']

        # 값 변경
        replacements = {
            '부재료비': '부자재',
            '동판': '부자재',
            '몰드비': '부자재',
            '스트로우': '부자재',
            '이산화탄소': '원자재'
        }
        bom_df['품목자산분류'] = bom_df['품목자산분류'].replace(replacements)

        # 추가 조건: '사전원가'에 값이 있고, '자재명'에 '추출-'이 포함된 경우
        조건_추출_원자재 = (bom_df['사전원가'].notna()) & (bom_df['자재명'].str.contains('추출-', na=False))
        bom_df.loc[조건_추출_원자재, '품목자산분류'] = '원자재'


        # 지정된 헤더만 선택
        selected_columns = [
            '구분','품목대분류', '품명', '품번', '규격','입수', '단위', '공정흐름차수명', '공정', 
            '품목자산분류','자재명', '자재번호', '자재규격', '단위.2', '소요량분자', '대표거래처', 
            '비고', '조달구분', 'BOM환산수량', '단가', '환산비용', '수율', 'loss율', 
            '단가_배전', '단가_미세', '배합원가', 
            'loss율_포장', '사전원가'
        ]
        bom_df = bom_df[selected_columns]  # 지정된 컬럼만 선택

        # 중간 결과 저장
        bom_df.to_csv(file_name, index=False, encoding='utf-8-sig')
        logging.info(f"중간 결과 저장 완료: '{file_name}'")

        # 조건에 맞는 '품번'만 추출
        조건 = (
            bom_df['품목대분류'].isin(['조제', '반제품']) &
            bom_df['공정흐름차수명'].isin(['노무비', '제조경비', '임가공비', '재료비'])
        )
        추출_품번 = bom_df.loc[조건, '품번'].drop_duplicates()

        # 조건에 맞는 '품번'에 해당하는 모든 행 추출
        필터링된_df = bom_df[bom_df['품번'].isin(추출_품번)]

        # 동일한 파일 이름에 덮어쓰기
        필터링된_df.to_csv(file_name, index=False, encoding='utf-8-sig')
        logging.info(f"최종 조건에 맞는 행 저장 완료: '{file_name}'")
        print(f"최종 조건에 맞는 행 저장 완료: '{file_name}'")

    except Exception as e:
        logging.error(f"파일 '{file_name}' 처리 중 오류 발생: {e}")
        print(f"파일 '{file_name}' 처리 중 오류 발생: {e}")
        
        
def main():
    try:
        # 고정된 입력 파일명 설정
        input_file = os.path.join(UPLOAD_DIR,"사전원가_조제.csv")

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
