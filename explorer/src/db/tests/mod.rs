mod model;

use crate::db::{
    chain_storable::{
        BlockDate, BlockId, ChainLength, EpochNumber, ExplorerVoteProposal, FragmentId, SlotId,
        StorableHash, VotePlanId,
    },
    endian::B32,
    schema::Txn,
    tests::model::TransactionSpec,
    Pristine,
};
use chain_addr::{self, Discrimination, Kind};
use chain_core::property::Fragment as _;
use chain_crypto::{Ed25519, KeyPair};
use chain_impl_mockchain::{
    chaintypes::ConsensusType,
    fragment::{ConfigParams, Fragment},
    transaction::{self, Input, Output, TransactionSignDataHash, TxBuilder},
};
use std::collections::{BTreeMap, BTreeSet};
use std::iter::FromIterator;

#[test]
fn sanakirja_test() {
    let pristine = Pristine::new_anon().unwrap();
    let mut model = model::Model::new();

    let mut mut_tx = pristine.mut_txn_begin().unwrap();

    let block0_id = BlockId::from([1u8; 32]);
    let block1_id = BlockId::from([2u8; 32]);
    let block1_date = BlockDate {
        epoch: EpochNumber::new(0),
        slot_id: SlotId::new(1),
    };

    let state = model.states.get(&model::Model::BLOCK0_ID).unwrap();

    mut_tx
        .add_block0(
            &model::Model::BLOCK0_PARENT_ID,
            &model::Model::BLOCK0_ID,
            state.fragments.iter(),
        )
        .unwrap();

    model.add_block(
        &block0_id,
        &block1_id,
        &block1_date,
        &ChainLength::new(1),
        vec![TransactionSpec::Transaction {
            from: vec![(0, 5000)],
            to: vec![3],
        }],
    );

    let branch1_id = StorableHash::from([2u8; 32]);
    let branch2_id = StorableHash::from([3u8; 32]);
    let branch3_id = StorableHash::from([4u8; 32]);

    let branch_config: BTreeMap<BlockId, (BlockId, Vec<TransactionSpec>)> = BTreeMap::from_iter([
        (
            branch1_id.clone(),
            (
                block0_id.clone(),
                vec![
                    TransactionSpec::Transaction {
                        from: vec![(0, 140), (5, 2500)],
                        to: vec![3, 6],
                    },
                    TransactionSpec::Transaction {
                        from: vec![(8, 3000)],
                        to: vec![1, 9],
                    },
                    TransactionSpec::VoteCast {
                        from: 7,
                        vote_plan: 0,
                        proposal: 3,
                        option: 1,
                    },
                    TransactionSpec::VoteCast {
                        from: 7,
                        vote_plan: 0,
                        proposal: 2,
                        option: 0,
                    },
                ],
            ),
        ),
        (
            branch2_id.clone(),
            (
                branch1_id.clone(),
                vec![TransactionSpec::Transaction {
                    from: vec![(1, 3000), (4, 5600)],
                    to: vec![3, 2],
                }],
            ),
        ),
        (
            branch3_id.clone(),
            (
                block0_id.clone(),
                vec![TransactionSpec::Transaction {
                    from: vec![(0, 50000)],
                    to: vec![7],
                }],
            ),
        ),
    ]);

    for (branch_id, (parent, spec)) in branch_config {
        let block_date = BlockDate {
            epoch: B32::new(0),
            slot_id: B32::new(1),
        };

        model.add_block(&parent, &branch_id, &block_date, &ChainLength::new(1), spec);

        let fragments = model.states.get(&branch_id).unwrap().fragments.clone();

        mut_tx
            .add_block(
                &parent,
                &branch_id,
                ChainLength::new(1),
                block_date,
                fragments.iter(),
            )
            .unwrap();
    }

    mut_tx.set_tip(branch2_id.clone()).unwrap();

    mut_tx.commit().unwrap();

    let txn = pristine.txn_begin().unwrap();

    assert_eq!(&txn.get_tip().unwrap(), &branch2_id);

    for (branch_id, branch) in model.get_state_refs() {
        for (address, fragments) in branch.transactions_by_address.iter() {
            let mut iter = txn
                .get_transactions_by_address(&branch_id, address)
                .unwrap()
                .unwrap();

            iter.seek_end().unwrap();

            assert_eq!(
                iter.rev()
                    .map(|v| v.map(|p| p.1).unwrap())
                    .collect::<Vec<&FragmentId>>(),
                fragments.iter().collect::<Vec<&FragmentId>>(),
            );
        }
    }

    assert!(txn
        .get_branches()
        .unwrap()
        .map(|result| result.unwrap())
        .eq(model.get_branches().iter().rev()));

    for block in [&block0_id, &branch1_id, &branch2_id, &branch3_id] {
        assert_eq!(
            txn.get_block_fragments(block)
                .unwrap()
                .map(|result| result.unwrap().1)
                .cloned()
                .collect::<Vec<FragmentId>>(),
            model
                .states
                .get(block)
                .unwrap()
                .fragments
                .iter()
                .map(|f| f.id().into())
                .collect::<Vec<FragmentId>>()
        );

        assert_eq!(
            txn.get_block_meta(block).unwrap(),
            model.get_block_meta(block)
        );

        for fragment_id in txn.get_block_fragments(block).unwrap() {
            let fragment_id = fragment_id.unwrap().1;
            let fragment = model.get_fragment(&fragment_id).unwrap();

            match fragment {
                Fragment::Initial(_) => {}
                Fragment::UpdateProposal(_) => {}
                Fragment::UpdateVote(_) => {}
                Fragment::OldUtxoDeclaration(_) => {}
                Fragment::Transaction(tx) => assert_transaction(&txn, fragment_id, tx),
                Fragment::OwnerStakeDelegation(tx) => assert_transaction(&txn, fragment_id, tx),
                Fragment::StakeDelegation(tx) => assert_transaction(&txn, fragment_id, tx),
                Fragment::PoolRegistration(tx) => assert_transaction(&txn, fragment_id, tx),
                Fragment::PoolRetirement(tx) => assert_transaction(&txn, fragment_id, tx),
                Fragment::PoolUpdate(tx) => assert_transaction(&txn, fragment_id, tx),
                Fragment::VotePlan(tx) => assert_transaction(&txn, fragment_id, tx),
                Fragment::VoteCast(tx) => {
                    let vote_cast = tx.as_slice().payload().into_payload();

                    let public_vote_cast = txn
                        .get_fragment_certificate(&fragment_id)
                        .unwrap()
                        .unwrap()
                        .clone()
                        .into_public_vote_cast()
                        .unwrap();

                    assert_eq!(
                        VotePlanId::from(vote_cast.vote_plan().clone()),
                        public_vote_cast.vote_plan_id
                    );

                    assert_transaction(&txn, fragment_id, tx)
                }
                Fragment::VoteTally(tx) => assert_transaction(&txn, fragment_id, tx),
                Fragment::EncryptedVoteTally(tx) => assert_transaction(&txn, fragment_id, tx),
            }

            fn assert_transaction<P>(
                txn: &Txn,
                fragment_id: &FragmentId,
                tx: &transaction::Transaction<P>,
            ) {
                let tx = tx.as_slice();

                assert!(tx.inputs().iter().eq(txn
                    .get_fragment_inputs(&fragment_id)
                    .unwrap()
                    .map(|i| i.unwrap().1.into())));

                assert!(tx.outputs().iter().eq(txn
                    .get_fragment_outputs(&fragment_id)
                    .unwrap()
                    .map(|o| o.unwrap().1.into())));
            }
        }

        for (vote_plan_id, vote_plan) in model.states.get(block).unwrap().vote_plans.iter() {
            let meta = txn.get_vote_plan_meta(vote_plan_id).unwrap().unwrap();

            assert_eq!(BlockDate::from(vote_plan.vote_start()), meta.vote_start);
            assert_eq!(BlockDate::from(vote_plan.vote_end()), meta.vote_end);
            assert_eq!(
                BlockDate::from(vote_plan.committee_end()),
                meta.committee_end
            );
            assert_eq!(vote_plan.payload_type(), meta.payload_type.into());

            assert!(
                vote_plan
                    .proposals()
                    .iter()
                    .map(ExplorerVoteProposal::from)
                    .eq(txn
                        .get_vote_plan_proposals(vote_plan_id)
                        .unwrap()
                        .map(|p| p.unwrap().1.clone())),
                "VotePlan proposals in explorer don't match raw certificate"
            );
        }
    }

    for chain_length in 0..=3 {
        assert_eq!(
            txn.get_blocks_by_chain_length(&ChainLength::new(chain_length))
                .unwrap()
                .map(|result| result.unwrap())
                .cloned()
                .collect::<BTreeSet<_>>(),
            model
                .get_blocks_by_chain_length(&ChainLength::new(chain_length))
                .map(Clone::clone)
                .unwrap_or_default(),
        );
    }
    // pub type StakePools = UDb<PoolId, StakePoolMeta>;
}
