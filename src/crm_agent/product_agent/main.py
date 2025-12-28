from __future__ import annotations

import argparse
import json

from crm_agent.product_agent.workflow import run_product_agent

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--run_id", required=True)
    p.add_argument("--top_k_products", type=int, default=3)
    args = p.parse_args()

    out = run_product_agent(args.run_id, top_k_products=args.top_k_products)
    print(json.dumps(out.get("summary", {}), ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
