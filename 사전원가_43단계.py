import pandas as pd
import os
import logging
from typing import Dict, List, Tuple

UPLOAD_DIR = "uploads"

def setup_logging() -> None:
    logging.basicConfig(
        filename="log_BOM_배전_조제.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def load_dataframes() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """BOM과 수율 데이터를 로드"""
    bom_df = pd.read_csv(os.path.join(UPLOAD_DIR,'BOM_배전_조제.csv'), encoding='utf-8-sig', low_memory=False)
    yield_df = pd.read_csv(os.path.join(UPLOAD_DIR,'수율.csv'), encoding='utf-8-sig', low_memory=False)
    return bom_df, yield_df

def update_dried_fruits(bom_df: pd.DataFrame, yield_df: pd.DataFrame) -> pd.DataFrame:
    """
    1. '자재명'에 '건조' 또는 '열풍'이 포함된 품번 추출
    2. '수율.csv'의 '대분류'가 '조제'이고 '비고'가 '건조과일'일 때 품번이 일치하면 '수율'과 'loss율'을 BOM 파일에 업데이트
    """
    # '자재명'에 '건조' 또는 '열풍' 포함된 품번 추출
    dried_fruits_condition = bom_df['자재명'].str.contains('건조|열풍', na=False)
    dried_fruits_parts = bom_df.loc[dried_fruits_condition, '품번']

    # '수율.csv'에서 조건에 맞는 데이터 필터링
    filtered_yield_df = yield_df[
        (yield_df['대분류'] == '조제') & (yield_df['비고'] == '건조과일')
    ][['품번', '수율', 'loss율']]

    # '품번' 기준으로 수율 및 loss율 매핑
    mapping = filtered_yield_df.set_index('품번').to_dict(orient='index')

    # 조건을 만족하는 품번만 업데이트
    for part in dried_fruits_parts:
        if part in mapping:
            # dried_fruits_condition을 이용해 조건을 추가
            bom_df.loc[(bom_df['품번'] == part) & dried_fruits_condition, ['수율', 'loss율']] = \
                mapping[part]['수율'], mapping[part]['loss율']

    logging.info("'건조과일' 관련 수율 및 loss율 업데이트 완료")
    return bom_df


def update_raw_materials(bom_df: pd.DataFrame, yield_df: pd.DataFrame) -> pd.DataFrame:
    """
    3. '대분류'가 '조제'이고 '구분'이 '부자재'인 데이터를 추출
    4. '품목자산분류'가 '부자재'이고 품번이 일치할 때 수율과 loss율 값을 BOM 파일에 업데이트
    """
    # '수율.csv'에서 조건에 맞는 데이터 필터링
    filtered_yield_df = yield_df[
        (yield_df['대분류'] == '조제') & (yield_df['구분'] == '부자재')
    ][['품번', '수율', 'loss율']].drop_duplicates(subset='품번', keep='first')

    # '품번' 기준으로 수율 및 loss율 매핑
    mapping = filtered_yield_df.set_index('품번').to_dict(orient='index')

    # 'BOM_배전_조제.csv'에서 '품목자산분류'가 '부자재'인 조건
    raw_material_condition = bom_df['품목자산분류'] == '부자재'
    for index, row in bom_df[raw_material_condition].iterrows():
        part_number = row['품번']
        if part_number in mapping:
            bom_df.loc[index, ['수율', 'loss율']] = mapping[part_number]['수율'], mapping[part_number]['loss율']

    logging.info("'부자재' 관련 수율 및 loss율 업데이트 완료")
    return bom_df

def update_raw_materials2(bom_df: pd.DataFrame, yield_df: pd.DataFrame) -> pd.DataFrame:
    """
    5. '대분류'가 '조제'이고 '구분'이 '부자재'인 데이터를 추출
    6. '공정'가 '스티커' 또는 '재활용분담금'이고 품번이 일치할 때 수율과 loss율 값을 BOM 파일에 업데이트
    """
    # '수율.csv'에서 조건에 맞는 데이터 필터링
    filtered_yield_df = yield_df[
        (yield_df['대분류'] == '조제') & (yield_df['구분'] == '부자재')
    ][['품번', '수율', 'loss율']].drop_duplicates(subset='품번', keep='first')

    # '품번' 기준으로 수율 및 loss율 매핑
    mapping = filtered_yield_df.set_index('품번').to_dict(orient='index')

    # 'BOM_배전_조제.csv'에서 '품목자산분류'가 '부자재'인 조건
    raw_material_condition = bom_df['공정'].isin(['스티커', '재활용분담금'])
    for index, row in bom_df[raw_material_condition].iterrows():
        part_number = row['품번']
        if part_number in mapping:
            bom_df.loc[index, ['수율', 'loss율']] = mapping[part_number]['수율'], mapping[part_number]['loss율']

    logging.info("'부자재' 관련 수율 및 loss율 업데이트 완료")
    return bom_df

def main():
    try:
        # 현재 스크립트 위치를 기준으로 작업 디렉토리 설정
        script_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(script_dir)

        # 로깅 설정
        setup_logging()

        # 데이터프레임 로드
        bom_df, yield_df = load_dataframes()

        # 건조/열풍 품목 관련 데이터 업데이트
        bom_df = update_dried_fruits(bom_df, yield_df)

        # 부자재 품목 관련 데이터 업데이트
        bom_df = update_raw_materials(bom_df, yield_df)
        bom_df = update_raw_materials2(bom_df, yield_df)

        # 결과 저장
        bom_df.to_csv(os.path.join(UPLOAD_DIR,'BOM_배전_조제.csv'), index=False, encoding='utf-8-sig')
        logging.info("BOM 원가 가공 작업이 완료되었습니다.")
        print("BOM 원가 가공 완료 - BOM_배전_조제.csv 파일 갱신 완료")

    except FileNotFoundError as e:
        logging.error(f"파일을 찾을 수 없습니다: {e}")
        print("오류: 파일을 찾을 수 없습니다.")
    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {e}")
        print("오류 발생, 로그 파일을 확인하세요.")

if __name__ == "__main__":
    main()
