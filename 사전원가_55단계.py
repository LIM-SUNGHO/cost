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
    filename="log_BOM_사전원가_조제.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():
    try:
        # 파일 경로 설정
        input_file = os.path.join(UPLOAD_DIR, '사전원가_조제.csv')

        # 파일 불러오기 (encoding='utf-8-sig')
        df = pd.read_csv(input_file, encoding='utf-8-sig',low_memory=False)
        logging.info("BOM 파일을 성공적으로 불러왔습니다.")

        # '조제' 헤더 생성 및 값 추가
        def calculate_ondoo(row):
            try:
                # '입수' 값이 없으면 기본값으로 1 설정 (빈칸, NaN, 0 모두 처리)
                입수 = row['입수'] if pd.notna(row['입수']) and row['입수'] != 0 else 1

                # 조건 1: '품목대분류'가 '조제'이고 '품목자산분류'가 '부자재'
                if row['품목대분류'] == '조제' and row['품목자산분류'] == '부자재':
                    return row['환산비용'] / 입수

                # 조건 2: '공정흐름차수명'이 '노무비' 또는 '제조경비'
                if row['품목대분류'] == '조제' and row['공정흐름차수명'] in ['노무비', '제조경비'] and row['구분'] in ['미세']:
                    return row['단가']

                # 조건 3: '공정'이 '스티커' 또는 '재활용분담금'
                if row['품목대분류'] == '조제' and row['공정'] in ['스티커', '재활용분담금'] and row['구분'] in ['미세']:
                    return row['단가']

                # 조건 4~8: '조달구분'이 '제작'인 경우 자재명과 세부 조건 처리
                if row['품목대분류'] == '조제' and row['조달구분'] == '제작' and row['구분'] in ['미세']:
                    if '미세' in row['자재명'] and row['BOM환산수량'] != 0:
                        return row['단가_미세'] * row['BOM환산수량'] / 입수
                    elif '배전' in row['자재명'] and row['BOM환산수량'] != 0:
                        return row['단가_배전'] * row['BOM환산수량'] / 입수

                # 조건 10: '구분' 값이 '미세'이 아닌 경우 및 '품목자산분류'가 '원자재'이고 '자재번호'가 특정 패턴
                if (
                    row['품목대분류'] == '조제'
                    and row['구분'] not in ['미세']
                    and row['품목자산분류'] == '원자재'
                    and pd.notna(row['자재번호'])
                    and bool(re.match(r'5\d[A|B]\d{5}', str(row['자재번호'])))
                ):
                    if row['수율']:
                        return row['환산비용'] / row['수율']

                # 조건 11: '구분' 값이 '미세'이 아닌 경우 및 '품목자산분류'가 '원자재'이고 '자재번호'가 특정 패턴이 아닌 경우
                if (
                    row['품목대분류'] == '조제'
                    and row['구분'] not in ['미세']
                    and row['품목자산분류'] == '원자재'
                    and not bool(re.match(r'5\d[A|B]\d{5}', str(row['자재번호'])))
                ):
                    return row['환산비용'] / 입수

                # 조건 12: '품목대분류'가 '조제'이고 '구분'값이 '미세'이며, '품목자산분류'가 '원자재' 이고, '자재번호'가 특정패턴이 아닌 경우
                if (
                    row['품목대분류'] == '조제'
                    and row['구분'] in ['미세']
                    and row['품목자산분류'] == '원자재'
                    and not bool(re.match(r'5\d[A|B]\d{5}', str(row['자재번호'])))
                ):
                    return row['환산비용'] / 입수

                # 조건 13: '구분' 값이 '미세'이 아닌 경우 및 '품목자산분류'가 '부자재'
                if row['품목대분류'] == '조제' and row['구분'] not in ['미세'] and row['품목자산분류'] == '부자재':
                    return row['환산비용']

                # 조건 14: '구분' 값이 '미세'이 아닌 경우 및 '공정'이 특정 값
                if row['품목대분류'] == '조제' and row['구분'] not in ['미세'] and row['공정'] in ['스티커', '재활용분담금']:
                    return row['단가']

                # 조건 15: '품목대분류'가 '조제'인 경우
                if row['품목대분류'] == '조제' and row['공정흐름차수명'] in ['노무비', '제조경비'] and row['구분'] not in ['미세']:
                    return row['단가']

                # 조건 16: '구분' 값이 '선물세트'이고 '품목자산분류'가 '0'이며, '품목소분류'가 '아워티(조제_NB)'일 때 '단가_스틱' * '소요량분자' 값 추출
                if (
                    row['구분'] in ['선물세트']
                and str(row['품목자산분류']).strip() == '0'
                    and row['품목소분류'] == '아워티(조제_NB)'
                ):
                    return row['단가_스틱'] * row['소요량분자']


                return None
            except KeyError as e:
                logging.warning(f"필드 누락: {e}")
                return None

        # 두 함수를 순차적으로 적용
        df['조제'] = df.apply(calculate_ondoo, axis=1)
        
        # 조건 17: '구분' 값이 '선물세트'이고 '품목자산분류'가 '부자재'이며, '품목소분류'가 '아워티(조제_NB)'일 때 '환산비용' 값을 추출
        condition_1 = (
            (df['구분'] == ('선물세트')) &
            (df['품목자산분류'].isin(['부자재'])) &
            (df['품목소분류'] == ('아워티(조제_NB)'))
        )
        df.loc[condition_1, '조제'] = df.loc[condition_1, '환산비용']
        
        # 조건 18: '구분' 값이 '선물세트'이고 '품목자산분류'가 '0'이며, '품목소분류'가 '반제품'일 때 0으로 추출
        condition_2 = (
            (df['구분'] == ('선물세트')) &
            (df['품목소분류'] == ('반제품'))
        )
        df.loc[condition_2, '조제'] = 0
        

        # '조제_loss' 헤더 생성
        df['조제_loss'] = df.apply(lambda row: row['조제'] * row['loss율'] if row['품목대분류'] == '조제' and row['구분'] else None, axis=1)

        # '사전원가_조제' 헤더 생성
        df['사전원가_조제'] = df.apply(lambda row: row['조제'] + row['조제_loss'] if row['품목대분류'] == '조제' and row['구분'] else None, axis=1)

        # 처리 결과 저장
        df.to_csv(input_file, index=False, encoding='utf-8-sig')
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
