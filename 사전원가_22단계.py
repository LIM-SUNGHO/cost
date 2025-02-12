import pandas as pd
import os
import logging
from typing import Dict, List, Tuple

UPLOAD_DIR = "uploads"

def setup_logging() -> None:
    logging.basicConfig(
        filename="log_BOM_배전_원두.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def load_dataframes() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """데이터프레임을 로드하고 전처리하는 함수"""
    bom_df = pd.read_csv(os.path.join(UPLOAD_DIR,'BOM_배전_원두.csv'), encoding='utf-8-sig', low_memory=False)
    yield_df = pd.read_csv(os.path.join(UPLOAD_DIR,'수율.csv'), encoding='utf-8-sig', low_memory=False)
    
    # 품번 컬럼을 문자열로 변환
    bom_df['품번'] = bom_df['품번'].astype(str)
    yield_df['품번'] = yield_df['품번'].astype(str)
    
    return bom_df, yield_df

def create_loss_rate_mapping(yield_df: pd.DataFrame) -> Dict[str, float]:
    """재활용분담금 loss율 매핑 생성"""
    filtered_df = yield_df[yield_df['비고'] == '재활용분담금'][['품번', 'loss율']]
    
    if filtered_df['품번'].duplicated().any():
        logging.warning("중복된 품번이 감지되었습니다. 첫 번째 값을 사용합니다.")
        filtered_df = filtered_df.drop_duplicates(subset='품번', keep='first')
    
    return filtered_df.set_index('품번')['loss율'].to_dict()

def update_loss_rate(bom_df: pd.DataFrame, condition: pd.Series, loss_rate_mapping: Dict[str, float]) -> pd.DataFrame:
    """조건에 따라 loss율 업데이트"""
    추출된_품번 = bom_df.loc[condition, '품번']
    추출된_자재번호 = bom_df.loc[condition, '자재번호']
    
    updated_loss율 = bom_df.loc[bom_df['품번'].isin(추출된_품번), '품번'].map(loss_rate_mapping)
    bom_df.loc[bom_df['자재번호'].isin(추출된_자재번호), 'loss율'] = updated_loss율.fillna(bom_df['loss율'])
    
    return bom_df

def process_special_cases(bom_df: pd.DataFrame, yield_df: pd.DataFrame) -> pd.DataFrame:
    """비닐과 박스(30kg) 특수 케이스 처리"""
    조건 = bom_df['공정'].isin(['비닐', '박스(30kg)'])
    yield_mapping = yield_df.set_index('품번')['loss율'].to_dict()
    
    # loss율 업데이트
    bom_df.loc[조건, 'loss율'] = bom_df.loc[조건, '품번'].map(yield_mapping)
    
    # loss율_배전 계산
    bom_df.loc[조건, 'loss율_배전'] = bom_df.loc[조건, '단가'] * bom_df.loc[조건, 'loss율']
    
    logging.info("비닐/박스 특수 케이스 처리 완료")
    return bom_df

def update_recycling_loss_rate(bom_df: pd.DataFrame, yield_df: pd.DataFrame) -> pd.DataFrame:
    """
    '수율.csv' 파일에서 '비고'이 '재활용분담금' 또는 '스티커'일 때 
    '품번'이 일치하는 경우 'loss율' 값을 채움
    """
    # '수율.csv'에서 '재활용분담금'과 '스티커' 필터링
    recycling_yield_df = yield_df[yield_df['비고'].isin(['재활용분담금', '스티커','트레이더스'])][['품번', 'loss율']]
    
    # 중복 처리 (필요시 경고 로깅)
    if recycling_yield_df['품번'].duplicated().any():
        logging.warning("재활용분담금 또는 스티커 품번 중복 발견. 첫 번째 값을 사용합니다.")
        recycling_yield_df = recycling_yield_df.drop_duplicates(subset='품번', keep='first')
    
    # 매핑 생성
    recycling_loss_rate_mapping = recycling_yield_df.set_index('품번')['loss율'].to_dict()
    
    # 'BOM_배전_원두.csv'에서 '공정'이 '재활용분담금' 또는 '스티커'인 경우 매핑 적용
    recycling_condition = bom_df['공정'].isin(['재활용분담금', '스티커','트레이더스'])
    bom_df.loc[recycling_condition, 'loss율'] = bom_df.loc[recycling_condition, '품번'].map(recycling_loss_rate_mapping)
    
    logging.info("'재활용분담금' 및 '스티커' 관련 loss율 업데이트 완료")
    return bom_df


def update_loss_rate_for_raw_materials(bom_df: pd.DataFrame, yield_df: pd.DataFrame) -> pd.DataFrame:
    """
    '수율.csv'에서 '대분류'가 '원두'이고 '구분'이 '부자재'인 경우, 품번별 loss율을 가져와
    'BOM_배전_원두.csv'에서 '품목대분류'가 '원두'이고 '품목자산분류'가 '부자재'인 행의 품번을 기준으로 loss율을 매핑
    """
    # 1. 수율 데이터 필터링: '대분류'가 '원두'이고 '구분'이 '부자재'
    filtered_yield_df = yield_df[
        (yield_df['대분류'] == '원두') & (yield_df['구분'] == '부자재')
    ][['품번', 'loss율']]
    
    # 중복된 품번 처리
    if filtered_yield_df['품번'].duplicated().any():
        logging.warning("수율 데이터에서 중복된 품번이 발견되었습니다. 첫 번째 값을 사용합니다.")
        filtered_yield_df = filtered_yield_df.drop_duplicates(subset='품번', keep='first')
    
    # 2. 품번을 키로, loss율을 값으로 하는 매핑 생성
    loss_rate_mapping = filtered_yield_df.set_index('품번')['loss율'].to_dict()
    
    # 3. BOM 데이터 필터링 조건: '품목대분류'가 '원두'이고 '품목자산분류'가 '부자재'
    bom_condition = (bom_df['품목대분류'] == '원두') & (bom_df['품목자산분류'] == '부자재')
    
    # 4. 품번이 일치하면 loss율을 매핑
    bom_df.loc[bom_condition, 'loss율'] = bom_df.loc[bom_condition, '품번'].map(loss_rate_mapping)
    
    logging.info("'원두' 및 '부자재' 조건에 맞는 loss율 매핑이 완료되었습니다.")
    return bom_df


def update_co2_loss_rate(bom_df: pd.DataFrame, yield_df: pd.DataFrame) -> pd.DataFrame:
    """'수율.csv' 파일에서 '비고'가 '이산화탄소'이고 '조달구분'이 '이산화탄소'인 경우 'loss율' 값을 채움"""
    # '수율.csv'에서 '비고'가 '이산화탄소' 필터링
    co2_yield_df = yield_df[yield_df['비고'] == '이산화탄소'][['품번', 'loss율']]
    
    # 중복 처리 (필요시 경고 로깅)
    if co2_yield_df['품번'].duplicated().any():
        logging.warning("이산화탄소 품번 중복 발견. 첫 번째 값을 사용합니다.")
        co2_yield_df = co2_yield_df.drop_duplicates(subset='품번', keep='first')
    
    # 매핑 생성
    co2_loss_rate_mapping = co2_yield_df.set_index('품번')['loss율'].to_dict()
    
    # 'BOM_배전_원두.csv'에서 '조달구분'이 '이산화탄소'인 경우 매핑 적용
    co2_condition = bom_df['조달구분'] == '이산화탄소'
    bom_df.loc[co2_condition, 'loss율'] = bom_df.loc[co2_condition, '품번'].map(co2_loss_rate_mapping)
    
    logging.info("'이산화탄소' 관련 loss율 업데이트 완료")
    return bom_df

def main():
    try:
        # 현재 스크립트 위치를 기준으로 작업 디렉토리 설정
        script_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(script_dir)
        
        setup_logging()
        bom_df, yield_df = load_dataframes()
        loss_rate_mapping = create_loss_rate_mapping(yield_df)
        
        # 조건별 loss율 업데이트
        conditions = {
            '자재명': lambda df: df['자재명'].str.contains('박스|케이스|봉투|스트로우', na=False),
            '공정흐름': lambda df: df['공정흐름차수명'].str.contains('재료비', na=False),
            '동판': lambda df: df['공정'].str.contains('동판', na=False)
        }
        
        for name, condition_func in conditions.items():
            bom_df = update_loss_rate(bom_df, condition_func(bom_df), loss_rate_mapping)
            logging.info(f"{name} 조건에 대한 loss율 업데이트 완료")
        
        # '재활용분담금' loss율 업데이트
        bom_df = update_recycling_loss_rate(bom_df, yield_df)
        
        # '원두' 및 '부자재' 조건의 loss율 업데이트
        bom_df = update_loss_rate_for_raw_materials(bom_df, yield_df)
        
        # '이산화탄소' loss율 업데이트
        bom_df = update_co2_loss_rate(bom_df, yield_df)
        
        # 특수 케이스 처리
        bom_df = process_special_cases(bom_df, yield_df)
        
        # 결과 저장
        bom_df.to_csv(os.path.join(UPLOAD_DIR,'BOM_배전_원두.csv'), index=False, encoding='utf-8-sig')
        logging.info("BOM 배전 가공 작업이 완료되었습니다.")
        print("BOM 배전 가공 완료 - BOM_배전_원두.csv 파일 갱신 완료")
        
    except FileNotFoundError as e:
        logging.error(f"파일을 찾을 수 없습니다: {e}")
        print("오류: 파일을 찾을 수 없습니다.")
    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {e}")
        print("오류 발생, 로그 파일을 확인하세요.")

if __name__ == "__main__":
    main()
