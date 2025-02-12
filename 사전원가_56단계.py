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
        df = pd.read_csv(input_file, encoding='utf-8-sig', low_memory=False)
        logging.info("BOM 파일을 성공적으로 불러왔습니다.")

        df['품번'] = df['품번'].astype(str)
        df['자재번호'] = df['자재번호'].astype(str)

        # '품번'별 '사전원가_조제' 합산 값 계산
        품번_list = ['21367702', '21367101', '21363840', '21363845', '21367708', '21363815']
        filtered_df = df[df['품번'].isin(품번_list)]
        sum_values = filtered_df.groupby('품번')['사전원가_조제'].sum()
        logging.info(f"'품번'별 '사전원가_조제' 합산 결과:\n{sum_values}")

        # '자재번호'에 환산비용 매치
        def map_cost(row):
            """품번별 합산 값을 자재번호에 매핑"""
            mapping = {
                '40101015': '21367702',
                '40101016': '21367101',
                '40101046': '21363840',
                '40101047': '21363845',
                '40101019': '21367708',
                '40101020': '21363815'
            }
            품번 = mapping.get(row['자재번호'], None)
            return sum_values.get(품번, pd.NA)

        # '환산비용' 열이 없는 경우 생성
        if '환산비용' not in df.columns:
            df['환산비용'] = pd.NA

        # 특정 자재번호에 대해서만 업데이트
        df['환산비용'] = df.apply(
            lambda row: map_cost(row) if row['자재번호'] in ['40101015', '40101016', '40101046', '40101047', '40101019', '40101020'] else row['환산비용'],
            axis=1
        )

        # 3) '자재번호'가 특정 값에 해당하는 '품번' 추출
        품번_subset = df[df['자재번호'].isin(['40101015', '40101016', '40101046', '40101047', '40101019', '40101020'])]['품번'].unique()
        logging.info(f"추출된 품번: {품번_subset}")

        # 4) 추출된 '품번'에 대한 행 필터링
        subset_df = df[df['품번'].isin(품번_subset)].copy()

        # 5) '조제' 헤더 값 계산 ('환산비용' * '입수')
        subset_df['조제'] = subset_df.apply(
            lambda row: row['환산비용'] * row['입수'] if row['자재번호'] in ['40101015', '40101016', '40101046', '40101047', '40101019', '40101020'] else row.get('조제', pd.NA),
            axis=1
        )

        # 6) '공정흐름차수명'이 '노무비'인 경우 '단가' 적용
        subset_df['조제'] = subset_df.apply(
            lambda row: row['단가'] if row.get('공정흐름차수명') == '노무비' else row['조제'],
            axis=1
        )

        # 7) '조제_loss' 계산
        subset_df['조제_loss'] = subset_df.apply(
            lambda row: row['조제'] * row['loss율'],
            axis=1
        )

        # 8) '사전원가_조제' 계산
        subset_df['사전원가_조제'] = subset_df.apply(
            lambda row: row['조제'] + row['조제_loss'],
            axis=1
        )

        # 기존 데이터에 업데이트
        df.update(subset_df)

        # 처리 결과 저장
        output_file = os.path.join(UPLOAD_DIR, '사전원가_조제.csv')
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
